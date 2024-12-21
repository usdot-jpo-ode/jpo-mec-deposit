package us.dot.its.jpo.ode.mec.deposit.imp;

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
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpProperties.PartnerApiProperties;
import us.dot.its.jpo.ode.mec.deposit.models.imp.ImpConfigData;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.AuthToken;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.AuthTokenRequest;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.ClientConnectionPostRequest;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.ClientConnectionResponse;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.ClientRegistrationPostRequest;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.ClientRegistrationResponse;
import us.dot.its.jpo.ode.mec.deposit.utils.DateJsonMapper;

/**
 * API client for interacting with the IMP Partner API.
 */
@Slf4j
public class ImpPartnerApi {
  protected static final ObjectMapper MAPPER = DateJsonMapper.getInstance();
  private final ImpProperties impProperties;
  private final PartnerApiProperties partnerApi;
  private final RestTemplate restTemplate;

  /**
   * Constructs a new IMP Partner API client.
   *
   * @param properties The IMP configuration properties
   */
  public ImpPartnerApi(ImpProperties properties) {
    this.impProperties = properties;
    this.partnerApi = properties.getPartnerApi();
    this.restTemplate = new RestTemplate();
    this.restTemplate.getMessageConverters().add(new MappingJackson2HttpMessageConverter());
  }

  /**
   * Registers this client with the IMP Partner API.
   *
   * @return Configuration data for the registered client
   */
  public ImpConfigData registerClientPartner() {
    try {
      ImpConfigData configData;
      String deviceId = null;
      boolean cacheRegistration = impProperties.isCacheRegistration();

      String configPath = impProperties.getCertificatePath() + "/config.json";
      String caCertPath = impProperties.getCertificatePath() + "/imp-ca.pem";
      String certPath = impProperties.getCertificatePath() + "/imp-cert.pem";
      String keyPath = impProperties.getCertificatePath() + "/imp-key.pem";

      if (!validRegistration(configPath) || !cacheRegistration) {
        log.info("Registering client partner");

        long startTime = System.currentTimeMillis();
        String token = getToken();
        long endTime = System.currentTimeMillis();
        log.info("Time to get token: {} ms", (endTime - startTime));

        startTime = System.currentTimeMillis();
        ClientRegistrationResponse registrationResponse = register(token);
        endTime = System.currentTimeMillis();
        log.info("Time to register: {} ms", (endTime - startTime));

        ImpUtil.writeToFile(caCertPath, registrationResponse.getCertificate().getCaPem());
        ImpUtil.writeToFile(certPath, registrationResponse.getCertificate().getCertPem());
        ImpUtil.writeToFile(keyPath, registrationResponse.getCertificate().getKeyPem());

        deviceId = registrationResponse.getDeviceID();

        startTime = System.currentTimeMillis();
        ClientConnectionResponse connectionResponse = connection(token, deviceId);
        endTime = System.currentTimeMillis();
        log.info("Time to connect: {} ms", (endTime - startTime));
        URI uri = new URI(connectionResponse.getMqttURL());
        // configData = new ConfigData(configPath, caCertPath, certPath, keyPath,
        // properties.getImpVendor(),
        // NetworkType.valueOf(properties.getImpNetworkType()), uri, deviceID);

        configData = ImpConfigData.builder().configFilePath(configPath).caCertPath(caCertPath)
            .clientCertPath(certPath).keyFilePath(keyPath).impVendor(impProperties.getVendor())
            .networkType(impProperties.getNetworkType()).impMqttUri(uri).deviceID(deviceId)
            .impSessionID(null).build();

        String configDataJson = MAPPER.writeValueAsString(configData);

        ImpUtil.writeToFile(configPath, configDataJson);
      } else {
        log.info("Client partner already registered, obtaining config data");

        String configDataJson = Files.readString(Paths.get(configPath));
        configData = MAPPER.readValue(configDataJson, ImpConfigData.class);
        deviceId = configData.getDeviceID();

        // startTime = System.currentTimeMillis();
        // ClientConnectionResponse connectionResponse = connection(token, deviceID);
        // endtime = System.currentTimeMillis();
        // log.info("Time to connect: " + (endtime - startTime) + " ms");

        // URI uri = new URI(connectionResponse.getMqttURL());
        // configData.setImpMqttUri(uri);
      }

      return configData;
    } catch (Exception e) {
      log.error("Error in registerClientPartner", e);
      return null;
    }
  }

  /**
   * Gets an authentication token from the IMP Partner API.
   *
   * @return The authentication token
   */
  public String getToken() {
    var request = new AuthTokenRequest(partnerApi.getUser(), partnerApi.getPass());

    HttpHeaders headers = new HttpHeaders();
    headers.set("Content-Type", "application/json");

    HttpEntity<AuthTokenRequest> entity = new HttpEntity<>(request, headers);

    AuthToken response = restTemplate.postForObject(partnerApi.getBaseUri() + "/auth/token", entity,
        AuthToken.class);

    return response.getAccessToken();
  }

  /**
   * Registers a new client with the IMP Partner API.
   *
   * @param token Authentication token
   * @return The registration response
   */
  public ClientRegistrationResponse register(String token) {
    ClientRegistrationPostRequest request = new ClientRegistrationPostRequest(
        impProperties.getClientType().toString(), impProperties.getClientSubType().toString());

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
   * Establishes a connection with the IMP Partner API.
   *
   * @param token Authentication token
   * @param deviceID Device identifier
   * @return The connection response
   */
  public ClientConnectionResponse connection(String token, String deviceID) {
    ClientConnectionPostRequest request = new ClientConnectionPostRequest(deviceID,
        partnerApi.getMecLatitude(), partnerApi.getMecLongitude(), impProperties.getNetworkType());

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
        ImpConfigData configData = MAPPER.readValue(configDataJson, ImpConfigData.class);

        if (configData.getDeviceID() != null
            && configData.getNetworkType() == impProperties.getNetworkType()) {
          valid = true;
        }
      } catch (IOException e) {
        log.error("validRegistration error: " + e.getStackTrace());
      }
    }
    return valid;
  }
}
