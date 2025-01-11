package us.dot.its.jpo.ode.mec.deposit.etx;

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
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties.PartnerApiProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxConfigData;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.AuthToken;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.AuthTokenRequest;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.ClearRequest;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.ClientConnectionPostRequest;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.ClientConnectionResponse;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.ClientRegistrationPostRequest;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.ClientRegistrationResponse;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.DepositRequest;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.DistributionType;
import us.dot.its.jpo.ode.mec.deposit.utils.DateJsonMapper;
import org.springframework.util.StringUtils;
import org.springframework.lang.Nullable;

/**
 * API client for interacting with the ETX Partner API. This class handles registration,
 * authentication, and data exchange with the ETX system.
 */
@Slf4j
@Component
public class EtxApi {
  private final ObjectMapper mapper;
  private final EtxProperties etxProperties;
  private final PartnerApiProperties partnerApi;
  private final RestTemplate restTemplate;

  /**
   * Constructs a new ETX Partner API client.
   *
   * @param properties The ETX configuration properties
   */
  public EtxApi(EtxProperties properties, RestTemplate restTemplate) {
    if (properties == null) {
      throw new IllegalArgumentException("EtxProperties cannot be null");
    }
    this.etxProperties = properties;
    this.partnerApi = properties.getPartnerApi();
    this.restTemplate = restTemplate != null ? restTemplate : createDefaultRestTemplate();
    this.mapper = DateJsonMapper.getInstance();
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
  public EtxConfigData registerClientPartner(String token) {
    if (!StringUtils.hasText(token)) {
      throw new IllegalArgumentException("Token cannot be null or empty");
    }

    try {
      boolean cacheRegistration = etxProperties.isCacheRegistration();
      String configPath = buildConfigPath();

      if (!validRegistration(configPath) || !cacheRegistration) {
        return handleNewRegistration(token, configPath);
      } else {
        return loadExistingConfiguration(configPath);
      }
    } catch (Exception e) {
      log.error("Failed to register client partner", e);
      return null;
    }
  }

  /**
   * Builds the path to the configuration file based on the certificate path property.
   *
   * @return The full path to the configuration file
   * @throws IllegalStateException if certificate path is not configured
   */
  private String buildConfigPath() {
    String basePath = etxProperties.getCertificatePath();
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
  private EtxConfigData handleNewRegistration(String token, String configPath) throws Exception {
    log.info("Initiating new client partner registration");

    String caCertPath = etxProperties.getCertificatePath() + "/etx-ca.pem";
    String certPath = etxProperties.getCertificatePath() + "/etx-cert.pem";
    String keyPath = etxProperties.getCertificatePath() + "/etx-key.pem";

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
    EtxConfigData configData =
        buildConfigData(configPath, caCertPath, certPath, keyPath, deviceId, uri);

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

  /**
   * Constructs configuration data object from registration and connection information.
   *
   * @param configPath Path to configuration file
   * @param caCertPath Path to CA certificate
   * @param certPath Path to client certificate
   * @param keyPath Path to private key
   * @param deviceId Device identifier from registration
   * @param uri MQTT URI from connection response
   * @return Constructed configuration data object
   */
  private EtxConfigData buildConfigData(String configPath, String caCertPath, String certPath,
      String keyPath, String deviceId, URI uri) {
    return EtxConfigData.builder().configFilePath(configPath).caCertPath(caCertPath)
        .clientCertPath(certPath).keyFilePath(keyPath).impVendor(etxProperties.getVendor())
        .networkType(etxProperties.getNetworkType()).etxMqttUri(uri).deviceID(deviceId)
        .etxSessionID(null).build();
  }

  private EtxConfigData loadExistingConfiguration(String configPath) throws IOException {
    log.info("Loading existing client partner configuration");
    String configDataJson = Files.readString(Paths.get(configPath));
    return mapper.readValue(configDataJson, EtxConfigData.class);
  }

  /**
   * Gets an authentication token from the ETX Partner API.
   *
   * @return The authentication token
   */
  @Nullable
  public AuthToken getToken() {
    if (!StringUtils.hasText(partnerApi.getUsername())
        || !StringUtils.hasText(partnerApi.getPassword())) {
      log.error("Username or password not configured");
      return null;
    }

    try {
      var request = new AuthTokenRequest(partnerApi.getUsername(), partnerApi.getPassword());
      HttpHeaders headers = createJsonHeaders();
      HttpEntity<AuthTokenRequest> entity = new HttpEntity<>(request, headers);

      return restTemplate.postForObject(partnerApi.getBaseUri() + "/auth/token", entity,
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
          restTemplate.exchange(partnerApi.getBaseUri() + "/prd/v2/registration", HttpMethod.POST,
              entity, ClientRegistrationResponse.class);

      return response.getBody();
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
    ClientConnectionPostRequest request = new ClientConnectionPostRequest(deviceID,
        partnerApi.getMecLatitude(), partnerApi.getMecLongitude(), etxProperties.getNetworkType());

    HttpHeaders headers = new HttpHeaders();
    headers.set("Authorization", "Bearer " + token);
    headers.set("Content-Type", "application/json");

    HttpEntity<ClientConnectionPostRequest> entity = new HttpEntity<>(request, headers);

    try {
      ResponseEntity<ClientConnectionResponse> response =
          restTemplate.exchange(partnerApi.getBaseUri() + "/prd/v2/connection", HttpMethod.POST,
              entity, ClientConnectionResponse.class);

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
        EtxConfigData configData = mapper.readValue(configDataJson, EtxConfigData.class);

        if (configData.getDeviceID() != null
            && configData.getNetworkType() == etxProperties.getNetworkType()) {
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
   * @param distributionType Type of distribution
   */
  public void deposit(String token, String asn1Hex, DistributionType distributionType) {
    DepositRequest request = new DepositRequest(asn1Hex, distributionType);

    HttpHeaders headers = new HttpHeaders();
    headers.set("Authorization", "Bearer " + token);
    headers.set("Content-Type", "application/json");

    HttpEntity<DepositRequest> entity = new HttpEntity<>(request, headers);

    try {
      ResponseEntity<Void> response =
          restTemplate.exchange(partnerApi.getBaseUri() + "/prd/v2/configurations/deposit",
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

  /**
   * Clears TIM messages from the ETX system.
   *
   * @param token Authentication token
   * @return true if clear operation was successful, false otherwise
   */
  public boolean clearTim(String token) {
    HttpHeaders headers = new HttpHeaders();
    headers.set("Authorization", "Bearer " + token);
    headers.set("Content-Type", "application/json");

    HttpEntity<ClearRequest> entity = new HttpEntity<>(new ClearRequest(true), headers);

    ResponseEntity<Void> response =
        restTemplate.exchange(partnerApi.getBaseUri() + "/prd/v2/configurations/clear",
            HttpMethod.POST, entity, Void.class);

    HttpStatusCode responseCode = response.getStatusCode();
    if (responseCode.is2xxSuccessful()) {
      log.info("Cleared inactive TIMs deployed on the VZ Configuration API");
      return true;
    } else {
      log.error("Failed to clear inactive TIMs deployed on the VZ Configuration API");
      return false;
    }
  }

  /**
   * Creates HTTP headers for JSON content.
   *
   * @return HttpHeaders configured for JSON content
   */
  private HttpHeaders createJsonHeaders() {
    HttpHeaders headers = new HttpHeaders();
    headers.set("Content-Type", "application/json");
    return headers;
  }

  /**
   * Creates HTTP headers for authenticated JSON requests.
   *
   * @param token Authentication token
   * @return HttpHeaders configured with authorization and JSON content type
   */
  private HttpHeaders createAuthJsonHeaders(String token) {
    HttpHeaders headers = createJsonHeaders();
    headers.set("Authorization", "Bearer " + token);
    return headers;
  }
}
