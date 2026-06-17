package us.dot.its.jpo.ode.mec.deposit.services.depositors.mqtt;

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
import us.dot.its.jpo.asn.j2735.r2024.SPAT.SPAT;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties.MecDepositMetrics;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties.DepositorProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties.EtxMqttBrokerProfile;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties.MqttBrokerDepositors;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties.MqttBrokers;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties.MqttDepositorProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties.SpatIntersectionFilterProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxUtil;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientSubType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttClientInfo;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.RegistrationConfiguration;
import us.dot.its.jpo.ode.mec.deposit.services.etx.mqtt.EtxMqttPublishService;
import us.dot.its.jpo.ode.mec.deposit.utils.MapRefPointCollector;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttTopicBuilder;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SpatMqttDepositorTest {

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
  private SpatMqttDepositor depositor;
  private ObjectMapper objectMapper;
  private String sampleSpatJson;
  private MockedStatic<EtxMqttTopicBuilder> mockedTopicBuilder;
  private MockedStatic<EtxUtil> mockedEtxUtil;
  private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'";

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
    when(etxProperties.passesSpatIntersectionFilter(any(), any(SPAT.class))).thenReturn(true);
    when(etxProperties.nmiMqttTopicPrecision()).thenReturn(7);
    when(etxProperties.getClientType()).thenReturn(EtxClientType.SOFTWARE);
    when(etxProperties.getClientSubType()).thenReturn(EtxClientSubType.APPLICATION);

    when(mqttProperties.getPrecision()).thenReturn(7);
    when(mqttProperties.getVendor()).thenReturn("test-vendor");
    when(mqttProperties.getMessageFormat()).thenReturn(EtxMqttMessageFormat.J2735);

    depositor = new SpatMqttDepositor(mecDepositProperties, etxProperties, mqttProperties,
        mqttService, registry, kafkaTemplate);
    objectMapper = new ObjectMapper();

    sampleSpatJson = new String(Files.readAllBytes(Paths.get(
        getClass().getClassLoader().getResource("sample_messages/sample-ode-spat.json").toURI())));

    mockedTopicBuilder = Mockito.mockStatic(EtxMqttTopicBuilder.class);

    mockedEtxUtil = Mockito.mockStatic(EtxUtil.class);

    RegistrationConfiguration mockConfig = new RegistrationConfiguration();
    EtxMqttClientInfo mockSessionInfo = new EtxMqttClientInfo();
    mockSessionInfo.setSessionId("test-session-0000");
    mockConfig.setEtxSessionID(mockSessionInfo);
    mockedEtxUtil.when(() -> EtxUtil.readConfigFile(any(String.class))).thenReturn(mockConfig);

    mockedTopicBuilder.when(() -> EtxMqttTopicBuilder.getSpatTopicList(any(SPAT.class),
        eq(mapDataCollector), anyString(), anyInt(), any(EtxMqttMessageFormat.class),
        eq(EtxClientType.SOFTWARE), eq(EtxClientSubType.APPLICATION)))
        .thenReturn(Set.of("test-topic"));

    ReflectionTestUtils.setField(depositor, "mapDataCollector", mapDataCollector);
  }

  @AfterEach
  void tearDown() {
    if (mockedTopicBuilder != null) {
      mockedTopicBuilder.close();
    }
    if (mockedEtxUtil != null) {
      mockedEtxUtil.close();
    }
  }

  private String createSpatTestMessage(String timestamp) throws JsonProcessingException {
    OdeMessageFrameData spatData =
        objectMapper.readValue(sampleSpatJson, OdeMessageFrameData.class);
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
    String staleTimestamp = "2020-01-01T00:00:00.000000Z";
    String message = createSpatTestMessage(staleTimestamp);

    depositor.spatDepositListener(message);

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(byte[].class), eq(false));

    double staleCount =
        registry.get("mec-deposit.etx.mqtt.stale").tag("message.type", "SPAT").counter().count();
    assert (staleCount > 0);
  }

  @Test
  void testSpatDepositListener_HandlesException() throws JsonProcessingException {
    doThrow(new RuntimeException("MQTT publish failed")).when(mqttService)
        .publishAsn1Bytes(anyString(), any(byte[].class), eq(false));

    String currentTime =
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT));
    String message = createSpatTestMessage(currentTime);

    depositor.spatDepositListener(message);

    verify(kafkaTemplate).send(eq("test-metrics-topic"),
        argThat(metricsJson -> metricsJson.contains("\"success\":false")
            && metricsJson.contains("\"errorMessage\":\"MQTT publish failed\"")));

    double errorCount =
        registry.get("mec-deposit.etx.mqtt.error").tag("message.type", "SPAT").counter().count();
    assert (errorCount > 0);
  }

  @Test
  void testSpatDepositListener_IntersectionFilter_Enabled_AllowedIntersection()
      throws JsonProcessingException {
    SpatMqttDepositor d = depositorWithSpatFilter(true, List.of(9709L), null);
    String currentTime =
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT));
    String message = createSpatTestMessage(currentTime);

    d.spatDepositListener(message);

    verify(mqttService).publishAsn1Bytes(eq("test-topic"), any(byte[].class), eq(false));
    verify(kafkaTemplate).send(eq("test-metrics-topic"), anyString());
  }

  @Test
  void testSpatDepositListener_IntersectionFilter_Enabled_DisallowedIntersection()
      throws JsonProcessingException {
    SpatMqttDepositor d = depositorWithSpatFilter(true, List.of(1111L), null);
    String currentTime =
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT));
    String message = createSpatTestMessage(currentTime);

    d.spatDepositListener(message);

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(byte[].class), eq(false));
  }

  @Test
  void testSpatDepositListener_IntersectionFilter_BlockedIntersection()
      throws JsonProcessingException {
    SpatMqttDepositor d = depositorWithSpatFilter(true, null, List.of(9709L));
    String currentTime =
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT));
    String message = createSpatTestMessage(currentTime);

    d.spatDepositListener(message);

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(byte[].class), eq(false));
  }

  @Test
  void testSpatDepositListener_IntersectionFilter_NonBlockedIntersection()
      throws JsonProcessingException {
    SpatMqttDepositor d = depositorWithSpatFilter(true, null, List.of(1234L));
    String currentTime =
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT));
    String message = createSpatTestMessage(currentTime);

    d.spatDepositListener(message);

    verify(mqttService).publishAsn1Bytes(eq("test-topic"), any(byte[].class), eq(false));
    verify(kafkaTemplate).send(eq("test-metrics-topic"), anyString());
  }

  @Test
  void testSpatDepositListener_IntersectionFilter_BlockedTakesPrecedenceOverAllowed()
      throws JsonProcessingException {
    SpatMqttDepositor d = depositorWithSpatFilter(true, List.of(9709L), List.of(9709L));
    String currentTime =
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT));
    String message = createSpatTestMessage(currentTime);

    d.spatDepositListener(message);

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(byte[].class), eq(false));
  }

  @Test
  void testSpatDepositListener_IntersectionFilter_EmptyAllowlistAllowsNonBlocked()
      throws JsonProcessingException {
    SpatMqttDepositor d = depositorWithSpatFilter(true, List.of(), List.of(1234L));
    String currentTime =
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT));
    String message = createSpatTestMessage(currentTime);

    d.spatDepositListener(message);

    verify(mqttService).publishAsn1Bytes(eq("test-topic"), any(byte[].class), eq(false));
    verify(kafkaTemplate).send(eq("test-metrics-topic"), anyString());
  }

  private SpatMqttDepositor depositorWithSpatFilter(boolean filterEnabled,
      List<Long> allowedIntersectionIds, List<Long> blockedIntersectionIds) {
    SpatIntersectionFilterProperties filt = SpatIntersectionFilterProperties.builder()
        .enabled(filterEnabled).allowedIntersectionIds(allowedIntersectionIds)
        .blockedIntersectionIds(blockedIntersectionIds).build();
    MqttDepositorProperties mqtt =
        MqttDepositorProperties.builder().enabled(true).intersectionFilter(filt).build();
    DepositorProperties spatDep = DepositorProperties.builder().mqtt(mqtt).build();
    MqttBrokerDepositors mbd =
        MqttBrokerDepositors.builder().staleMessageThreshold(5000).spat(spatDep).build();
    EtxMqttBrokerProfile etxProf = EtxMqttBrokerProfile.builder().depositors(mbd).build();
    MqttBrokers mb = MqttBrokers.builder().etx(etxProf).build();
    EtxProperties realEtx = EtxProperties.builder().mqttBrokers(mb)
        .clientType(EtxClientType.SOFTWARE).clientSubType(EtxClientSubType.APPLICATION).build();
    SpatMqttDepositor d = new SpatMqttDepositor(mecDepositProperties, realEtx, mqttProperties,
        mqttService, registry, kafkaTemplate);
    ReflectionTestUtils.setField(d, "mapDataCollector", mapDataCollector);
    return d;
  }
}
