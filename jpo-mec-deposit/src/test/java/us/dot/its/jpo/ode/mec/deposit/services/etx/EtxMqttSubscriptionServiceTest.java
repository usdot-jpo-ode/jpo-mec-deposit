package us.dot.its.jpo.ode.mec.deposit.services.etx;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxUtil;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.PartnerApiProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttClientInfo;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.RegistrationConfiguration;
import us.dot.its.jpo.ode.mec.deposit.services.etx.mqtt.EtxMqttSubscriptionService;

@ExtendWith(MockitoExtension.class)
class EtxMqttSubscriptionServiceTest {

  @Mock
  private PartnerApiProperties mockPartnerApiProperties;

  @Mock
  private Message<String> mockMessage;

  @Mock
  private MessageHeaders mockHeaders;

  @Mock
  private ObjectMapper mockMapper;

  @InjectMocks
  private EtxMqttSubscriptionService service;

  @BeforeEach
  void setUp() {
    // Set up common mock behavior
    lenient().when(mockMessage.getHeaders()).thenReturn(mockHeaders);
    lenient().when(mockMessage.getPayload()).thenReturn("{}");
    lenient().when(mockPartnerApiProperties.getCertificatePath()).thenReturn("/test/path");
  }

  @Test
  void testHandleMessage_ClientInfo_Success() throws JsonProcessingException {
    // Arrange
    String clientInfoJson = "{\"sessionId\":\"test-session\"}";
    RegistrationConfiguration mockConfigData = new RegistrationConfiguration();
    mockConfigData.setConfigFilePath("/test/path/config.json");
    EtxMqttClientInfo mockClientInfo = new EtxMqttClientInfo();
    mockClientInfo.setSessionId("test-session");

    when(mockMessage.getPayload()).thenReturn(clientInfoJson);
    when(mockHeaders.get("mqtt_receivedTopic")).thenReturn("vzimp/1/ClientInfo");

    try (MockedStatic<EtxUtil> mockedEtxUtil = mockStatic(EtxUtil.class)) {
      // Mock the readConfigFile call
      mockedEtxUtil.when(() -> EtxUtil.readConfigFile(anyString())).thenReturn(mockConfigData);

      // Act
      service.handleMessage(mockMessage);

      // Verify
      verify(mockMessage, Mockito.times(2)).getHeaders();
      verify(mockHeaders).get("mqtt_receivedTopic");
      verify(mockMessage).getPayload();

      // Verify that writeToFile was called with the correct arguments
      mockedEtxUtil.verify(() -> EtxUtil.writeToFile(eq("/test/path/config.json"), anyString()));
    }
  }

  @Test
  void testHandleMessage_UnhandledTopic() {
    // Arrange
    when(mockMessage.getHeaders()).thenReturn(mockHeaders);
    when(mockHeaders.get("mqtt_receivedTopic")).thenReturn("unhandled/topic");
    when(mockMessage.getPayload()).thenReturn("test payload");

    // Act
    service.handleMessage(mockMessage);

    // Assert
    verify(mockMessage, Mockito.times(2)).getHeaders();
    verify(mockHeaders, Mockito.times(1)).get("mqtt_receivedTopic");
  }

  @Test
  void testHandleMessage_ClientInfo_InvalidJson() throws JsonProcessingException {
    // Arrange
    String malformedJson = "{\"clientId\":\"test\", malformed}";
    when(mockMessage.getPayload()).thenReturn(malformedJson);
    when(mockHeaders.get("mqtt_receivedTopic")).thenReturn("vzimp/1/ClientInfo");

    // Act & Assert
    assertDoesNotThrow(() -> service.handleMessage(mockMessage));

    // Verify basic interactions
    verify(mockMessage, Mockito.times(2)).getHeaders();
    verify(mockHeaders).get("mqtt_receivedTopic");
    verify(mockMessage).getPayload();
  }

  @Test
  void testHandleMessage_ClientInfo_ConfigWriteError() throws JsonProcessingException {
    // Arrange
    String clientInfoJson = "{\"sessionId\":\"test-session\"}";
    RegistrationConfiguration mockConfigData = new RegistrationConfiguration();
    mockConfigData.setConfigFilePath("/test/path/config.json");
    EtxMqttClientInfo mockClientInfo = new EtxMqttClientInfo();
    mockClientInfo.setSessionId("test-session");

    when(mockMessage.getPayload()).thenReturn(clientInfoJson);
    when(mockHeaders.get("mqtt_receivedTopic")).thenReturn("vzimp/1/ClientInfo");

    try (MockedStatic<EtxUtil> mockedEtxUtil = mockStatic(EtxUtil.class)) {
      // Mock the readConfigFile call
      mockedEtxUtil.when(() -> EtxUtil.readConfigFile(anyString())).thenReturn(mockConfigData);

      // Mock the writeToFile call to throw exception
      mockedEtxUtil.when(() -> EtxUtil.writeToFile(anyString(), anyString()))
          .thenThrow(new RuntimeException("Write error"));

      // Act
      service.handleMessage(mockMessage);

      // Verify
      verify(mockMessage, Mockito.times(2)).getHeaders();
      verify(mockHeaders).get("mqtt_receivedTopic");
      verify(mockMessage).getPayload();

      // Verify EtxUtil calls
      mockedEtxUtil.verify(() -> EtxUtil.readConfigFile(anyString()));
      mockedEtxUtil.verify(() -> EtxUtil.writeToFile(eq("/test/path/config.json"), anyString()));
    }
  }
}
