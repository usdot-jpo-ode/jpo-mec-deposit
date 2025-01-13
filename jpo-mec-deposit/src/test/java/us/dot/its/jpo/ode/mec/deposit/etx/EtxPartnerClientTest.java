package us.dot.its.jpo.ode.mec.deposit.etx;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpEntity;
import org.springframework.web.client.RestTemplate;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties.PartnerApiProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.AuthToken;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the ETX API client. Tests the interaction with the ETX Partner API endpoints
 * including authentication, registration, and data operations.
 */
@ExtendWith(MockitoExtension.class)
class EtxPartnerClientTest {
  @Mock
  private RestTemplate restTemplate;

  @Mock
  private EtxProperties etxProperties;

  private EtxPartnerClient etxApi;

  @BeforeEach
  void setUp() {
    when(etxProperties.getPartnerApi()).thenReturn(new PartnerApiProperties());
    etxApi = new EtxPartnerClient(etxProperties, restTemplate);
  }

  @Test
  void getToken_WithValidCredentials_ReturnsToken() {
    // Arrange
    PartnerApiProperties partnerApi = new PartnerApiProperties();
    partnerApi.setUsername("user");
    partnerApi.setPassword("pass");
    when(etxProperties.getPartnerApi()).thenReturn(partnerApi);

    AuthToken expectedToken = new AuthToken();
    expectedToken.setAccessToken("token123");

    when(restTemplate.postForObject(anyString(), any(HttpEntity.class), eq(AuthToken.class)))
        .thenReturn(expectedToken);

    // Act
    AuthToken result = etxApi.getToken();

    // Assert
    assertNotNull(result);
    assertEquals("token123", result.getAccessToken());
  }
}
