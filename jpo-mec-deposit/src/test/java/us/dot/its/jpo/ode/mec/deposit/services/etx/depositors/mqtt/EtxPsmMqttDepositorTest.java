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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import us.dot.its.jpo.ode.mec.deposit.utils.DateJsonMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.net.URISyntaxException;
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
class EtxPsmMqttDepositorTest {

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
  private EtxPsmMqttDepositor depositor;
  private ObjectMapper objectMapper;
  private String samplePsmJson;

  private MockedStatic<EtxMqttTopicBuilder> mockedTopicBuilder;

  @BeforeEach
  void setUp() throws IOException, URISyntaxException {
    registry = new SimpleMeterRegistry();

    MecDepositMetrics metrics = new MecDepositMetrics();
    metrics.setKafkaTopic("test-metrics-topic");
    metrics.setEnabled(true);
    when(mecDepositProperties.getMetrics()).thenReturn(metrics);

    int staleMessageThreshold = 5000;
    when(etxProperties.resolveStaleMessageThresholdMs()).thenReturn(staleMessageThreshold);
    when(etxProperties.isMqttDepositorEnabled(any(), anyString())).thenReturn(true);
    when(etxProperties.nmiMqttTopicPrecision()).thenReturn(7);
    when(etxProperties.getClientType()).thenReturn(EtxClientType.SOFTWARE);
    when(etxProperties.getClientSubType()).thenReturn(EtxClientSubType.APPLICATION);

    when(mqttProperties.getPrecision()).thenReturn(7);
    when(mqttProperties.getVendor()).thenReturn("test-vendor");
    when(mqttProperties.getMessageFormat()).thenReturn(EtxMqttMessageFormat.J2735);

    depositor = new EtxPsmMqttDepositor(mecDepositProperties, etxProperties, mqttProperties,
        mqttService, registry, kafkaTemplate);
    objectMapper = DateJsonMapper.getInstance();

    samplePsmJson = new String(Files.readAllBytes(Paths.get(
        getClass().getClassLoader().getResource("sample_messages/sample-ode-psm.json").toURI())));

    mockedTopicBuilder = Mockito.mockStatic(EtxMqttTopicBuilder.class);
    mockedTopicBuilder.when(() -> EtxMqttTopicBuilder.buildRegionalTopic(any(EtxMessageType.class),
        anyDouble(), anyDouble(), anyInt(), anyString(), any(EtxMqttMessageFormat.class),
        eq(EtxClientType.SOFTWARE), eq(EtxClientSubType.APPLICATION))).thenReturn("test-topic-psm");
  }

  @AfterEach
  void tearDown() {
    if (mockedTopicBuilder != null) {
      mockedTopicBuilder.close();
    }
  }

  @Test
  void testPsmDepositListener() throws JsonProcessingException {
    OdeMessageFrameData psmData = objectMapper.readValue(samplePsmJson, OdeMessageFrameData.class);
    String currentTime =
        LocalDateTime.now(ZoneOffset.UTC).atZone(ZoneOffset.UTC).toInstant().toString();
    psmData.getMetadata().setOdeReceivedAt(currentTime);
    String message = objectMapper.writeValueAsString(psmData);

    depositor.psmDepositListener(message);

    verify(mqttService).publishAsn1Bytes(eq("test-topic-psm"), any(byte[].class), eq(false));
    verify(kafkaTemplate).send(eq("test-metrics-topic"), anyString());
  }

  @Test
  void testPsmDepositListenerWithGeoRoutedFormat() throws JsonProcessingException {
    String currentTime = LocalDateTime.now(ZoneOffset.UTC)
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"));
    OdeMessageFrameData psmData = objectMapper.readValue(samplePsmJson, OdeMessageFrameData.class);
    psmData.getMetadata().setOdeReceivedAt(currentTime);
    String message = objectMapper.writeValueAsString(psmData);

    when(mqttProperties.getMessageFormat()).thenReturn(EtxMqttMessageFormat.J2735_GR);

    depositor.psmDepositListener(message);

    verify(mqttService).publishAsn1Bytes(eq("test-topic-psm"), any(byte[].class), eq(false));
    verify(kafkaTemplate).send(eq("test-metrics-topic"), anyString());
  }

  @Test
  void testPsmDepositListener_StaleMessage() throws JsonProcessingException {
    OdeMessageFrameData psmData = objectMapper.readValue(samplePsmJson, OdeMessageFrameData.class);
    psmData.getMetadata().setOdeReceivedAt("2020-01-01T00:00:00.000Z");
    String message = objectMapper.writeValueAsString(psmData);

    depositor.psmDepositListener(message);

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(byte[].class), eq(false));

    double staleCount =
        registry.get("mec-deposit.etx.mqtt.stale").tag("message.type", "PSM").counter().count();
    assert (staleCount > 0);
  }

  @Test
  void testPsmDepositListener_HandlesException() throws JsonProcessingException {
    OdeMessageFrameData psmData = objectMapper.readValue(samplePsmJson, OdeMessageFrameData.class);
    String currentTime = LocalDateTime.now(ZoneOffset.UTC)
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"));
    psmData.getMetadata().setOdeReceivedAt(currentTime);
    String message = objectMapper.writeValueAsString(psmData);

    doThrow(new RuntimeException("MQTT publish failed")).when(mqttService)
        .publishAsn1Bytes(anyString(), any(byte[].class), eq(false));

    depositor.psmDepositListener(message);

    verify(kafkaTemplate).send(eq("test-metrics-topic"),
        argThat(metricsJson -> metricsJson.contains("\"success\":false")
            && metricsJson.contains("\"errorMessage\":\"MQTT publish failed\"")));

    double errorCount =
        registry.get("mec-deposit.etx.mqtt.error").tag("message.type", "PSM").counter().count();
    assert (errorCount > 0);
  }
}
