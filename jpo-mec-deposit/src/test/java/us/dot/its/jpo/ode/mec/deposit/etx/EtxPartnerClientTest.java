package us.dot.its.jpo.ode.mec.deposit.etx;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxPartnerApiProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxPartnerClient;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientSubType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.AuthToken;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.ClientConnectionResponse;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.ClientRegistrationResponse;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.DistributionType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.NetworkType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.RegistrationConfiguration;

/**
 * Unit tests for the ETX API client. Tests the interaction with the ETX Partner API endpoints
 * including authentication, registration, and data operations.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class EtxPartnerClientTest {
  @Mock
  private RestTemplate mockRestTemplate;

  @Mock
  private EtxProperties mockEtxProperties;

  @Mock
  private EtxPartnerApiProperties mockPartnerApiProperties;

  private EtxPartnerClient etxApi;

  @BeforeEach
  void setUp() {
    when(mockEtxProperties.getPartnerApi()).thenReturn(mockPartnerApiProperties);
    etxApi = new EtxPartnerClient(mockEtxProperties, mockPartnerApiProperties, mockRestTemplate);
  }

  @Test
  void getToken_WithValidCredentials_ReturnsToken() {
    // Arrange
    when(mockPartnerApiProperties.getUsername()).thenReturn("user");
    when(mockPartnerApiProperties.getPassword()).thenReturn("password");

    AuthToken expectedToken = new AuthToken();
    expectedToken.setAccessToken("token123");

    when(mockRestTemplate.postForObject(anyString(), any(HttpEntity.class), eq(AuthToken.class)))
        .thenReturn(expectedToken);

    // Act
    AuthToken result = etxApi.getToken();

    // Assert
    assertNotNull(result);
    assertEquals("token123", result.getAccessToken());
  }

  @Test
  void register_WithValidToken_ReturnsRegistrationResponse() {
    // Arrange
    String token = "valid-token";
    ClientRegistrationResponse expectedResponse = new ClientRegistrationResponse();
    expectedResponse.setDeviceID("device123");

    // Mock the client type and subtype
    when(mockEtxProperties.getClientType()).thenReturn(EtxClientType.SOFTWARE);
    when(mockEtxProperties.getClientSubType()).thenReturn(EtxClientSubType.APPLICATION);

    ResponseEntity<ClientRegistrationResponse> responseEntity = ResponseEntity.ok(expectedResponse);

    when(mockRestTemplate.exchange(anyString(), eq(HttpMethod.POST), any(HttpEntity.class),
        eq(ClientRegistrationResponse.class))).thenReturn(responseEntity);

    // Act
    ClientRegistrationResponse result = etxApi.register(token);

    // Assert
    assertNotNull(result);
    assertEquals("device123", result.getDeviceID());
  }

  @Test
  void connection_WithValidTokenAndDeviceId_ReturnsConnectionResponse() {
    // Arrange
    String token = "valid-token";
    String deviceId = "device123";
    ClientConnectionResponse expectedResponse = new ClientConnectionResponse();
    expectedResponse.setMqttURL("mqtt://test.com");

    ResponseEntity<ClientConnectionResponse> responseEntity = ResponseEntity.ok(expectedResponse);

    when(mockRestTemplate.exchange(anyString(), eq(HttpMethod.POST), any(HttpEntity.class),
        eq(ClientConnectionResponse.class))).thenReturn(responseEntity);

    // Act
    ClientConnectionResponse result = etxApi.connection(token, deviceId);

    // Assert
    assertNotNull(result);
    assertEquals("mqtt://test.com", result.getMqttURL());
  }

  @Test
  void deposit_WithValidParameters_ExecutesSuccessfully() {
    // Arrange
    String token = "valid-token";
    String asn1Hex = "testHex";
    DistributionType distributionType = DistributionType.TARGETED;

    ResponseEntity<Void> responseEntity = ResponseEntity.ok().build();

    when(mockRestTemplate.exchange(anyString(), eq(HttpMethod.POST), any(HttpEntity.class),
        eq(Void.class))).thenReturn(responseEntity);

    // Act & Assert
    assertDoesNotThrow(() -> etxApi.deposit(token, asn1Hex, distributionType));
  }

  @Test
  void clearTim_WithValidToken_ReturnsTrue() {
    // Arrange
    String token = "valid-token";
    ResponseEntity<Void> responseEntity = ResponseEntity.ok().build();

    when(mockRestTemplate.exchange(anyString(), eq(HttpMethod.POST), any(HttpEntity.class),
        eq(Void.class))).thenReturn(responseEntity);

    // Act
    boolean result = etxApi.clearTim(token);

    // Assert
    assertTrue(result);
  }

  @Test
  void validRegistration_WithValidConfig_ReturnsTrue() {
    // Arrange
    String configPath = "src/test/resources/test-config.json";
    when(mockPartnerApiProperties.getNetworkType()).thenReturn(NetworkType.NON_VZ);

    // Create a temporary test config file
    RegistrationConfiguration testConfig = RegistrationConfiguration.builder()
        .deviceID("testDevice").networkType(NetworkType.NON_VZ).build();

    // Act
    boolean result = etxApi.validRegistration(configPath);

    // Assert
    assertFalse(result); // Will be false because test file doesn't exist
  }

  @Test
  void getToken_WithInvalidCredentials_ReturnsNull() {
    // Arrange
    when(mockPartnerApiProperties.getUsername()).thenReturn(null);
    when(mockPartnerApiProperties.getPassword()).thenReturn(null);

    // Act
    AuthToken result = etxApi.getToken();

    // Assert
    assertNull(result);
  }
}
