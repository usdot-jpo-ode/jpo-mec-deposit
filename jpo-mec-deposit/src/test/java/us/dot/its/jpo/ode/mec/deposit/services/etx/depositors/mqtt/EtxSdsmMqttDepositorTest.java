package us.dot.its.jpo.ode.mec.deposit.services.etx.depositors.mqtt;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.kafka.core.KafkaTemplate;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties.MecDepositMetrics;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties.EtxDepositors;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientSubType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.services.etx.EtxMqttPublishService;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttTopicBuilder;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class EtxSdsmMqttDepositorTest {

  @Mock
  private MecDepositProperties mecDepositProperties;

  @Mock
  private EtxProperties etxProperties;

  @Mock
  private EtxMqttProperties mqttProperties;

  @Mock
  private EtxMqttPublishService mqttService;

  @Mock
  private KafkaTemplate<String, String> kafkaTemplate;

  @Mock
  private Timer timer;

  @Mock
  private Counter counter;

  private MeterRegistry registry;
  private EtxSdsmMqttDepositor depositor;
  private ObjectMapper objectMapper;
  private String sampleSdsmJson;

  private MockedStatic<EtxMqttTopicBuilder> mockedTopicBuilder;

  @BeforeEach
  void setUp() throws Exception {
    // Use SimpleMeterRegistry instead of mocking
    registry = new SimpleMeterRegistry();

    // Configure MecDepositProperties metrics
    MecDepositMetrics metrics = new MecDepositMetrics();
    metrics.setKafkaTopic("test-metrics-topic");
    metrics.setEnabled(true);
    when(mecDepositProperties.getMetrics()).thenReturn(metrics);

    // Configure stale message threshold
    int staleMessageThreshold = 5000; // 5 seconds
    EtxDepositors depositors = new EtxDepositors();
    depositors.setStaleMessageThreshold(staleMessageThreshold);
    when(etxProperties.getDepositors()).thenReturn(depositors);
    when(etxProperties.getClientType()).thenReturn(EtxClientType.SOFTWARE);
    when(etxProperties.getClientSubType()).thenReturn(EtxClientSubType.APPLICATION);

    when(mqttProperties.getPrecision()).thenReturn(7);
    when(mqttProperties.getVendor()).thenReturn("test-vendor");
    when(mqttProperties.getMessageFormat()).thenReturn(EtxMqttMessageFormat.J2735);

    depositor = new EtxSdsmMqttDepositor(mecDepositProperties, etxProperties, mqttProperties,
        mqttService, registry, kafkaTemplate);
    objectMapper = new ObjectMapper();

    // Load sample SDSM JSON from resources
    sampleSdsmJson = new String(Files.readAllBytes(Paths.get(
        getClass().getClassLoader().getResource("sample_messages/sample-ode-sdsm.json").toURI())));

    // Add static method mocking
    mockedTopicBuilder = Mockito.mockStatic(EtxMqttTopicBuilder.class);
    mockedTopicBuilder.when(() -> EtxMqttTopicBuilder.buildRegionalTopic(any(EtxMessageType.class),
        anyDouble(), anyDouble(), anyInt(), anyString(), any(EtxMqttMessageFormat.class),
        eq(EtxClientType.SOFTWARE), eq(EtxClientSubType.APPLICATION))).thenReturn("test-topic");
  }

  @AfterEach
  void tearDown() {
    if (mockedTopicBuilder != null) {
      mockedTopicBuilder.close();
    }
  }

  @Test
  void testSdsmDepositListener() throws Exception {
    // Arrange
    OdeMessageFrameData sdsmData =
        objectMapper.readValue(sampleSdsmJson, OdeMessageFrameData.class);
    String currentTime =
        LocalDateTime.now(ZoneOffset.UTC).atZone(ZoneOffset.UTC).toInstant().toString();
    sdsmData.getMetadata().setOdeReceivedAt(currentTime);
    String message = objectMapper.writeValueAsString(sdsmData);

    depositor.sdsmDepositListener(message);

    verify(mqttService).publishAsn1Bytes(eq("test-topic"), any(byte[].class), eq(false));
    verify(kafkaTemplate).send(eq("test-metrics-topic"), anyString());
  }

  @Test
  void testSdsmDepositListenerWithGeoRoutedFormat() throws Exception {
    // Arrange
    String currentTime = LocalDateTime.now(ZoneOffset.UTC)
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"));
    OdeMessageFrameData sdsmData =
        objectMapper.readValue(sampleSdsmJson, OdeMessageFrameData.class);
    sdsmData.getMetadata().setOdeReceivedAt(currentTime);
    String message = objectMapper.writeValueAsString(sdsmData);

    when(mqttProperties.getMessageFormat()).thenReturn(EtxMqttMessageFormat.J2735_GR);

    depositor.sdsmDepositListener(message);

    // Assert
    verify(mqttService).publishAsn1Bytes(eq("test-topic"), any(byte[].class), eq(false));
    verify(kafkaTemplate).send(eq("test-metrics-topic"), anyString());
  }

  @Test
  void testSdsmDepositListener_StaleMessage() throws Exception {
    // Arrange
    OdeMessageFrameData sdsmData =
        objectMapper.readValue(sampleSdsmJson, OdeMessageFrameData.class);
    sdsmData.getMetadata().setOdeReceivedAt("2020-01-01T00:00:00.000Z"); // Stale timestamp
    String message = objectMapper.writeValueAsString(sdsmData);

    // Act
    depositor.sdsmDepositListener(message);

    // Assert
    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(byte[].class), eq(false));

    // Verify stale message counter was incremented
    double staleCount =
        registry.get("mec-deposit.etx.mqtt.stale").tag("message.type", "SDSM").counter().count();
    assert (staleCount > 0);
  }

  @Test
  void testSdsmDepositListener_HandlesException() throws Exception {
    // Arrange
    OdeMessageFrameData sdsmData =
        objectMapper.readValue(sampleSdsmJson, OdeMessageFrameData.class);
    String currentTime = LocalDateTime.now(ZoneOffset.UTC)
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"));
    sdsmData.getMetadata().setOdeReceivedAt(currentTime);
    String message = objectMapper.writeValueAsString(sdsmData);

    // Simulate an exception during MQTT publish
    doThrow(new RuntimeException("MQTT publish failed")).when(mqttService)
        .publishAsn1Bytes(anyString(), any(byte[].class), eq(false));

    // Act
    depositor.sdsmDepositListener(message);

    // Assert
    // Verify error metrics were published to Kafka
    verify(kafkaTemplate).send(eq("test-metrics-topic"),
        argThat(metricsJson -> metricsJson.contains("\"success\":false")
            && metricsJson.contains("\"errorMessage\":\"MQTT publish failed\"")));

    // Verify error counter was incremented
    double errorCount =
        registry.get("mec-deposit.etx.mqtt.error").tag("message.type", "SDSM").counter().count();
    assert (errorCount > 0);
  }
}
