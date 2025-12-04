package us.dot.its.jpo.ode.mec.deposit.etx.partner;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxUtil;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.AuthToken;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.AuthTokenRequest;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.ClientConnectionPostRequest;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.ClientConnectionResponse;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.ClientRegistrationGetResponse;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.ClientRegistrationPostRequest;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.ClientRegistrationResponse;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.DepositRequest;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.RegistrationConfiguration;

/**
 * API client for interacting with the ETX Partner API. This class handles registration,
 * authentication, and data exchange with the ETX system.
 */
@Slf4j
@Component
public class EtxPartnerClient {
  private final ObjectMapper mapper;
  private final EtxProperties etxProperties;
  private final EtxPartnerApiProperties partnerApiProperties;
  private final RestTemplate restTemplate;

  /**
   * Constructs a new ETX Partner API client.
   *
   * @param properties The ETX configuration properties
   */
  public EtxPartnerClient(EtxProperties properties, EtxPartnerApiProperties partnerApi,
      RestTemplate restTemplate, ObjectMapper mapper) {
    this.etxProperties = properties;
    this.partnerApiProperties = partnerApi;
    this.restTemplate = restTemplate != null ? restTemplate : createDefaultRestTemplate();
    this.mapper = mapper;
  }

  /**
   * Creates a default RestTemplate configured with JSON message conversion capabilities.
   *
   * @return A new RestTemplate instance
   */
  private RestTemplate createDefaultRestTemplate() {
    RestTemplate template = new RestTemplate();
    template.getMessageConverters().add(new MappingJackson2HttpMessageConverter());
    return template;
  }

  /**
   * Registers this client with the ETX Partner API.
   *
   * @return Configuration data for the registered client
   */
  @Nullable
  public RegistrationConfiguration registerClientPartner(String token) throws Exception {
    if (!StringUtils.hasText(token)) {
      throw new IllegalArgumentException("Token cannot be null or empty");
    }

    try {
      boolean cacheRegistration = partnerApiProperties.isCacheRegistration();
      String configPath = buildConfigPath();

      RegistrationConfiguration configData = null;

      // If caching is enabled, check if the registration already exists and return the
      // configuration if it is still valid
      if (cacheRegistration) {
        try {
          configData = loadExistingConfiguration(configPath);
          if (configData != null) {
            ClientRegistrationGetResponse registrationResponse =
                getRegistration(token, configData.getDeviceID());

            // If registration not found (404) or null, proceed with new registration
            if (registrationResponse == null) {
              log.info(
                  "Existing registration not found or invalid, proceeding with new registration");
              configData = null;
            } else {
              ClientConnectionResponse connectionResponse =
                  connection(token, registrationResponse.getDeviceID());
              if (connectionResponse == null || connectionResponse.getMqttURL() == null) {
                throw new RuntimeException("Connection failed - null or invalid response");
              }

              configData.setEtxMqttUri(new URI(connectionResponse.getMqttURL()));
              EtxUtil.writeToFile(configPath, mapper.writeValueAsString(configData));
              return configData;
            }
          }
        } catch (IOException e) {
          log.info("Failed to load existing configuration, continuing with new registration");
          configData = null;
        }
      }

      return handleNewRegistration(token, configPath);
    } catch (Exception e) {
      log.error("Failed to register client partner", e);
      throw e;
    }
  }

  /**
   * Builds the path to the configuration file based on the certificate path property.
   *
   * @return The full path to the configuration file
   * @throws IllegalStateException if certificate path is not configured
   */
  private String buildConfigPath() {
    String basePath = partnerApiProperties.getCertificatePath();
    if (!StringUtils.hasText(basePath)) {
      throw new IllegalStateException("Certificate path not configured");
    }
    return basePath + "/config.json";
  }

  /**
   * Handles the registration of a new client partner with the ETX system. This includes certificate
   * generation and connection establishment.
   *
   * @param token Authentication token for the API
   * @param configPath Path where configuration should be stored
   * @return Configuration data for the new registration
   * @throws Exception if registration, certificate writing, or connection fails
   */
  private RegistrationConfiguration handleNewRegistration(String token, String configPath)
      throws Exception {
    log.info("Initiating new client partner registration");

    String caCertPath = partnerApiProperties.getCertificatePath() + "/etx-ca.pem";
    String certPath = partnerApiProperties.getCertificatePath() + "/etx-cert.pem";
    String keyPath = partnerApiProperties.getCertificatePath() + "/etx-key.pem";

    ClientRegistrationResponse registrationResponse = register(token);
    if (registrationResponse == null || registrationResponse.getDeviceID() == null) {
      throw new RuntimeException("Registration failed - null or invalid response");
    }

    String deviceId = registrationResponse.getDeviceID();
    writeCertificates(registrationResponse, caCertPath, certPath, keyPath);

    ClientConnectionResponse connectionResponse = connection(token, deviceId);
    if (connectionResponse == null || connectionResponse.getMqttURL() == null) {
      throw new RuntimeException("Connection failed - null or invalid response");
    }

    URI uri = new URI(connectionResponse.getMqttURL());
    RegistrationConfiguration configData = RegistrationConfiguration.builder()
        .configFilePath(configPath).caCertPath(caCertPath).clientCertPath(certPath)
        .keyFilePath(keyPath).etxVendor(partnerApiProperties.getVendor())
        .clientType(etxProperties.getClientType()).clientSubType(etxProperties.getClientSubType())
        .networkType(partnerApiProperties.getNetworkType()).etxMqttUri(uri).deviceID(deviceId)
        .mecLatitude(partnerApiProperties.getMecLatitude())
        .mecLongitude(partnerApiProperties.getMecLongitude()).build();

    String configDataJson = mapper.writeValueAsString(configData);
    EtxUtil.writeToFile(configPath, configDataJson);

    return configData;
  }

  /**
   * Writes the certificate data received during registration to files.
   *
   * @param response Registration response containing certificate data
   * @param caCertPath Path where CA certificate should be written
   * @param certPath Path where client certificate should be written
   * @param keyPath Path where private key should be written
   * @throws IOException if writing certificates fails
   * @throws RuntimeException if certificate data is missing from response
   */
  private void writeCertificates(ClientRegistrationResponse response, String caCertPath,
      String certPath, String keyPath) throws IOException {
    if (response.getCertificate() == null) {
      throw new RuntimeException("Registration response missing certificate data");
    }

    EtxUtil.writeToFile(caCertPath, response.getCertificate().getCaPem());
    EtxUtil.writeToFile(certPath, response.getCertificate().getCertPem());
    EtxUtil.writeToFile(keyPath, response.getCertificate().getKeyPem());
  }

  private RegistrationConfiguration loadExistingConfiguration(String configPath)
      throws IOException {
    log.info("Loading existing client partner configuration");
    String configDataJson = Files.readString(Paths.get(configPath));
    return mapper.readValue(configDataJson, RegistrationConfiguration.class);
  }

  /**
   * Gets an authentication token from the ETX Partner API.
   *
   * @return The authentication token
   */
  @Nullable
  public AuthToken getToken() {
    if (!StringUtils.hasText(partnerApiProperties.getUsername())
        || !StringUtils.hasText(partnerApiProperties.getPassword())) {
      log.error("Username or password not configured");
      return null;
    }

    try {
      var request = new AuthTokenRequest(partnerApiProperties.getUsername(),
          partnerApiProperties.getPassword());
      HttpHeaders headers = new HttpHeaders();
      headers.set("Content-Type", "application/json");
      HttpEntity<AuthTokenRequest> entity = new HttpEntity<>(request, headers);

      return restTemplate.postForObject(partnerApiProperties.getBaseUri() + "/auth/token", entity,
          AuthToken.class);
    } catch (Exception e) {
      log.error("Failed to obtain auth token", e);
      return null;
    }
  }

  /**
   * Registers a new client with the ETX Partner API.
   *
   * @param token Authentication token
   * @return The registration response
   */
  public ClientRegistrationResponse register(String token) {
    ClientRegistrationPostRequest request = new ClientRegistrationPostRequest(
        etxProperties.getClientType().toString(), etxProperties.getClientSubType().toString());

    HttpHeaders headers = new HttpHeaders();
    headers.set("Authorization", "Bearer " + token);
    headers.set("Content-Type", "application/json");

    HttpEntity<ClientRegistrationPostRequest> entity = new HttpEntity<>(request, headers);

    try {
      ResponseEntity<ClientRegistrationResponse> response =
          restTemplate.exchange(partnerApiProperties.getBaseUri() + "/prd/v2/registration",
              HttpMethod.POST, entity, ClientRegistrationResponse.class);

      return response.getBody();
    } catch (HttpClientErrorException.Unauthorized e) {
      log.error("Unauthorized error: " + e.getStackTrace());
      return null;
    }
  }

  /**
   * Registers a new client with the ETX Partner API.
   *
   * @param token Authentication token
   * @return The registration response
   */
  public ClientRegistrationGetResponse getRegistration(String token, String deviceID) {
    HttpHeaders headers = new HttpHeaders();
    headers.set("Authorization", "Bearer " + token);
    headers.set("Content-Type", "application/json");

    HttpEntity<Void> entity = new HttpEntity<>(headers);

    try {
      ResponseEntity<ClientRegistrationGetResponse> response = restTemplate.exchange(
          partnerApiProperties.getBaseUri() + "/prd/v2/registration?DeviceID=" + deviceID,
          HttpMethod.GET, entity, ClientRegistrationGetResponse.class);

      return response.getBody();
    } catch (HttpClientErrorException.NotFound e) {
      log.info("Registration not found (404) for deviceID: {}, will create new registration",
          deviceID);
      return null;
    } catch (HttpClientErrorException.Unauthorized e) {
      log.error("Unauthorized error: " + e.getStackTrace());
      return null;
    }
  }

  /**
   * Establishes a connection with the ETX Partner API.
   *
   * @param token Authentication token
   * @param deviceID Device identifier
   * @return The connection response
   */
  public ClientConnectionResponse connection(String token, String deviceID) {
    ClientConnectionPostRequest request =
        new ClientConnectionPostRequest(deviceID, partnerApiProperties.getMecLatitude(),
            partnerApiProperties.getMecLongitude(), partnerApiProperties.getNetworkType());

    HttpHeaders headers = new HttpHeaders();
    headers.set("Authorization", "Bearer " + token);
    headers.set("Content-Type", "application/json");

    HttpEntity<ClientConnectionPostRequest> entity = new HttpEntity<>(request, headers);

    try {
      ResponseEntity<ClientConnectionResponse> response =
          restTemplate.exchange(partnerApiProperties.getBaseUri() + "/prd/v2/connection",
              HttpMethod.POST, entity, ClientConnectionResponse.class);

      return response.getBody();
    } catch (HttpClientErrorException.Unauthorized e) {
      log.error("Unauthorized error: " + e.getStackTrace());
      return null;
    }
  }

  /**
   * Checks if there is a valid registration at the given path.
   *
   * @param configPath Path to the configuration file
   * @return true if registration is valid, false otherwise
   */
  public boolean validRegistration(String configPath) {
    boolean valid = false;
    File file = new File(configPath);
    if (file.exists()) {
      try {
        String configDataJson = Files.readString(Paths.get(configPath));
        RegistrationConfiguration configData =
            mapper.readValue(configDataJson, RegistrationConfiguration.class);

        if (configData.getDeviceID() != null
            && configData.getEtxVendor().equals(partnerApiProperties.getVendor())
            && configData.getNetworkType().equals(partnerApiProperties.getNetworkType())
            && configData.getClientType().equals(etxProperties.getClientType())
            && configData.getClientSubType().equals(etxProperties.getClientSubType())
            && configData.getMecLatitude().equals(partnerApiProperties.getMecLatitude())
            && configData.getMecLongitude().equals(partnerApiProperties.getMecLongitude())) {
          valid = true;
        }
      } catch (IOException e) {
        log.error("validRegistration error: " + e.getStackTrace());
      }
    }
    return valid;
  }

  /**
   * Deposits data to the ETX Partner API.
   *
   * @param token Authentication token
   * @param asn1Hex ASN.1 hex string to deposit
   */
  public void deposit(String token, String asn1Hex) {
    DepositRequest request = new DepositRequest(asn1Hex);

    HttpHeaders headers = new HttpHeaders();
    headers.set("Authorization", "Bearer " + token);
    headers.set("Content-Type", "application/json");

    HttpEntity<DepositRequest> entity = new HttpEntity<>(request, headers);

    try {
      ResponseEntity<Void> response =
          restTemplate.exchange(partnerApiProperties.getBaseUri() + "/prd/v2/deposit/geofence",
              HttpMethod.POST, entity, Void.class);
      HttpStatusCode responseCode = response.getStatusCode();
      if (responseCode.is2xxSuccessful()) {
        log.debug("Deposit response: " + responseCode);
      } else {
        log.error("Deposit response: " + responseCode);
      }
    } catch (HttpClientErrorException.Unauthorized e) {
      log.error("Unauthorized deposit error: " + e.getStackTrace());
    }
  }
}
