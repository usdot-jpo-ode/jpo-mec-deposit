package us.dot.its.jpo.ode.mec.deposit.services.etx.depositors.mqtt;

import static org.mockito.ArgumentMatchers.any;
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
import java.util.List;
import java.util.Set;
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
import org.springframework.test.util.ReflectionTestUtils;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties.MecDepositMetrics;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties.DepositorProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties.EtxDepositors;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties.MqttDepositorProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties.SpatIntersectionFilterProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientSubType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.services.etx.EtxMqttPublishService;
import us.dot.its.jpo.ode.mec.deposit.utils.MapRefPointCollector;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttTopicBuilder;
import us.dot.its.jpo.ode.model.OdeSpatData;
import us.dot.its.jpo.ode.plugin.j2735.J2735SPAT;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class EtxSpatMqttDepositorTest {

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

  @Mock
  private MapRefPointCollector mapDataCollector;

  private MeterRegistry registry;
  private EtxSpatMqttDepositor depositor;
  private ObjectMapper objectMapper;
  private String sampleSpatJson;
  private MockedStatic<EtxMqttTopicBuilder> mockedTopicBuilder;
  private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'";

  @BeforeEach
  void setUp() throws IOException, URISyntaxException {
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

    MqttDepositorProperties mqtt = new MqttDepositorProperties();
    SpatIntersectionFilterProperties intersectionFilter = new SpatIntersectionFilterProperties();
    intersectionFilter.setEnabled(false);

    mqtt.setIntersectionFilter(intersectionFilter);
    depositors.setSpat(new DepositorProperties(mqtt, null));

    when(etxProperties.getDepositors()).thenReturn(depositors);
    when(etxProperties.getClientType()).thenReturn(EtxClientType.SOFTWARE);
    when(etxProperties.getClientSubType()).thenReturn(EtxClientSubType.APPLICATION);

    when(mqttProperties.getPrecision()).thenReturn(7);
    when(mqttProperties.getVendor()).thenReturn("test-vendor");
    when(mqttProperties.getMessageFormat()).thenReturn(EtxMqttMessageFormat.J2735);

    depositor = new EtxSpatMqttDepositor(mecDepositProperties, etxProperties, mqttProperties,
        mqttService, registry, kafkaTemplate);
    objectMapper = new ObjectMapper();

    // Load sample Spat JSON from resources
    sampleSpatJson = new String(Files.readAllBytes(Paths.get(
        getClass().getClassLoader().getResource("sample_messages/sample-ode-spat.json").toURI())));

    mockedTopicBuilder = Mockito.mockStatic(EtxMqttTopicBuilder.class);

    // More specific mock setup
    mockedTopicBuilder.when(() -> EtxMqttTopicBuilder.getSpatTopicList(any(J2735SPAT.class),
        eq(mapDataCollector), anyString(), anyInt(), any(EtxMqttMessageFormat.class),
        eq(EtxClientType.SOFTWARE), eq(EtxClientSubType.APPLICATION)))
        .thenReturn(Set.of("test-topic"));

    // Inject the mocked mapDataCollector using reflection
    ReflectionTestUtils.setField(depositor, "mapDataCollector", mapDataCollector);
  }

  @AfterEach
  void tearDown() {
    if (mockedTopicBuilder != null) {
      mockedTopicBuilder.close();
    }
  }

  private String createSpatTestMessage(String timestamp) throws JsonProcessingException {
    OdeSpatData spatData = objectMapper.readValue(sampleSpatJson, OdeSpatData.class);
    spatData.getMetadata().setOdeReceivedAt(timestamp);
    return objectMapper.writeValueAsString(spatData);
  }

  @Test
  void testSpatDepositListener() throws JsonProcessingException {
    String currentTime =
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT));
    String message = createSpatTestMessage(currentTime);
    when(mqttProperties.getMessageFormat()).thenReturn(EtxMqttMessageFormat.J2735);

    depositor.spatDepositListener(message);


    verify(mqttService).publishAsn1Bytes(eq("test-topic"), any(byte[].class), eq(false));
    verify(kafkaTemplate).send(eq("test-metrics-topic"), anyString());
  }

  @Test
  void testSpatDepositListenerWithGeoRoutedFormat() throws JsonProcessingException {
    // Arrange
    String currentTime =
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT));
    String message = createSpatTestMessage(currentTime);
    when(mqttProperties.getMessageFormat()).thenReturn(EtxMqttMessageFormat.J2735_GR);

    depositor.spatDepositListener(message);


    verify(mqttService).publishAsn1Bytes(anyString(), any(byte[].class), eq(false));
    verify(kafkaTemplate).send(eq("test-metrics-topic"), anyString());
  }

  @Test
  void testSpatDepositListener_StaleMessage() throws JsonProcessingException {
    String staleTimestamp = "2020-01-01T00:00:00.000000Z"; // Stale timestamp
    String message = createSpatTestMessage(staleTimestamp);

    depositor.spatDepositListener(message);

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(byte[].class), eq(false));

    // Verify stale message counter was incremented
    double staleCount =
        registry.get("mec-deposit.etx.mqtt.stale").tag("message.type", "SPAT").counter().count();
    assert (staleCount > 0);
  }

  @Test
  void testSpatDepositListener_HandlesException() throws JsonProcessingException {
    // Simulate an exception during MQTT publish
    doThrow(new RuntimeException("MQTT publish failed")).when(mqttService)
        .publishAsn1Bytes(anyString(), any(byte[].class), eq(false));

    String currentTime =
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT));
    String message = createSpatTestMessage(currentTime);

    depositor.spatDepositListener(message);

    // Verify error metrics were published to Kafka
    verify(kafkaTemplate).send(eq("test-metrics-topic"),
        argThat(metricsJson -> metricsJson.contains("\"success\":false")
            && metricsJson.contains("\"errorMessage\":\"MQTT publish failed\"")));

    // Verify error counter was incremented
    double errorCount =
        registry.get("mec-deposit.etx.mqtt.error").tag("message.type", "SPAT").counter().count();
    assert (errorCount > 0);
  }

  @Test
  void testSpatDepositListener_IntersectionFilter_Enabled_AllowedIntersection()
      throws JsonProcessingException {
    // override the intersection filter
    ReflectionTestUtils.setField(depositor, "intersectionFilterEnabled", true);
    ReflectionTestUtils.setField(depositor, "allowedIntersectionIds", List.of(9709));

    String currentTime =
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT));
    String message = createSpatTestMessage(currentTime);

    depositor.spatDepositListener(message);

    verify(mqttService).publishAsn1Bytes(eq("test-topic"), any(byte[].class), eq(false));
    verify(kafkaTemplate).send(eq("test-metrics-topic"), anyString());
  }

  @Test
  void testSpatDepositListener_IntersectionFilter_Enabled_DisallowedIntersection()
      throws JsonProcessingException {
    // override the intersection filter
    ReflectionTestUtils.setField(depositor, "intersectionFilterEnabled", true);
    ReflectionTestUtils.setField(depositor, "allowedIntersectionIds", List.of(1111));

    String currentTime =
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT));
    String message = createSpatTestMessage(currentTime);

    depositor.spatDepositListener(message);

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(byte[].class), eq(false));
  }

  @Test
  void testSpatDepositListener_IntersectionFilter_BlockedIntersection()
      throws JsonProcessingException {
    // Configure filter with blocked intersection ID 9709 (from sample message)
    ReflectionTestUtils.setField(depositor, "intersectionFilterEnabled", true);
    ReflectionTestUtils.setField(depositor, "blockedIntersectionIds", List.of(9709));

    String currentTime =
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT));
    String message = createSpatTestMessage(currentTime);

    depositor.spatDepositListener(message);

    // message should be filtered out
    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(byte[].class), eq(false));
  }

  @Test
  void testSpatDepositListener_IntersectionFilter_NonBlockedIntersection()
      throws JsonProcessingException {
    // Configure filter with different blocked intersection ID
    ReflectionTestUtils.setField(depositor, "intersectionFilterEnabled", true);
    ReflectionTestUtils.setField(depositor, "blockedIntersectionIds", List.of(1234));

    String currentTime =
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT));
    String message = createSpatTestMessage(currentTime);

    depositor.spatDepositListener(message);

    // message should be processed
    verify(mqttService).publishAsn1Bytes(eq("test-topic"), any(byte[].class), eq(false));
    verify(kafkaTemplate).send(eq("test-metrics-topic"), anyString());
  }

  @Test
  void testSpatDepositListener_IntersectionFilter_BlockedTakesPrecedenceOverAllowed()
      throws JsonProcessingException {
    // Configure filter with intersection both allowed and blocked
    ReflectionTestUtils.setField(depositor, "intersectionFilterEnabled", true);
    ReflectionTestUtils.setField(depositor, "allowedIntersectionIds", List.of(9709));
    ReflectionTestUtils.setField(depositor, "blockedIntersectionIds", List.of(9709));

    String currentTime =
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT));
    String message = createSpatTestMessage(currentTime);

    depositor.spatDepositListener(message);

    // message should be filtered out due to being blocked
    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(byte[].class), eq(false));
  }

  @Test
  void testSpatDepositListener_IntersectionFilter_EmptyAllowlistAllowsNonBlocked()
      throws JsonProcessingException {
    // Configure filter with empty allowlist and non-matching blocked list
    ReflectionTestUtils.setField(depositor, "intersectionFilterEnabled", true);
    ReflectionTestUtils.setField(depositor, "allowedIntersectionIds", List.of());
    ReflectionTestUtils.setField(depositor, "blockedIntersectionIds", List.of(1234));

    String currentTime =
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT));
    String message = createSpatTestMessage(currentTime);

    depositor.spatDepositListener(message);

    // message should be processed since it's not blocked
    verify(mqttService).publishAsn1Bytes(eq("test-topic"), any(byte[].class), eq(false));
    verify(kafkaTemplate).send(eq("test-metrics-topic"), anyString());
  }

}
