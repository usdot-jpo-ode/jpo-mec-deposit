package us.dot.its.jpo.ode.mec.deposit.etx.depositors.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttService;
import us.dot.its.jpo.ode.mec.deposit.utils.MapRefPointCollector;
import us.dot.its.jpo.ode.model.OdeSpatData;
import us.dot.its.jpo.ode.plugin.j2735.OdePosition3D;

class ImpSpatMqttDepositorTest {

  private ImpSpatMqttDepositor depositor;
  private MeterRegistry meterRegistry;
  private String sampleSpatJson;
  private ObjectMapper mapper;

  @Mock
  private EtxProperties etxProperties;

  @Mock
  private EtxMqttService mqttService;

  @Mock
  private EtxMqttProperties mqttConfig;

  @Mock
  private MapRefPointCollector mapDataCollector;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    meterRegistry = new SimpleMeterRegistry();
    mapper = new ObjectMapper();

    // Setup mock properties
    when(etxProperties.getMqtt()).thenReturn(mqttConfig);
    when(mqttConfig.getStaleMessageThreshold()).thenReturn(5000); // 5 seconds

    // Mock successful MQTT publish
    when(mqttService.publishAsn1Bytes(anyString(), any(), eq(false)))
        .thenReturn(CompletableFuture.completedFuture(null));

    // Mock MapRefPointCollector
    OdePosition3D mockPosition = new OdePosition3D();
    mockPosition.setLatitude(BigDecimal.valueOf(42.0));
    mockPosition.setLongitude(BigDecimal.valueOf(-83.0));
    when(mapDataCollector.getIntersectionRefPoint(anyString())).thenReturn(mockPosition);

    depositor = new ImpSpatMqttDepositor(etxProperties, mqttService, meterRegistry);
    // Inject the mock MapRefPointCollector using reflection
    ReflectionTestUtils.setField(depositor, "mapDataCollector", mapDataCollector);

    // Load sample SPAT JSON from resources
    sampleSpatJson = new String(Files.readAllBytes(Paths.get(
        getClass().getClassLoader().getResource("sample_messages/sample-ode-spat.json").toURI())));
  }

  @Test
  void testSpatDepositListener_SuccessfulDeposit() throws Exception {
    // Load and modify the JSON to have a recent timestamp
    OdeSpatData spatData = mapper.readValue(sampleSpatJson, OdeSpatData.class);
    spatData.getMetadata().setOdeReceivedAt(
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
    String recentSpatJson = mapper.writeValueAsString(spatData);

    depositor.spatDepositListener(recentSpatJson);

    verify(mqttService, atLeastOnce()).publishAsn1Bytes(anyString(), any(), eq(false));
    assert (meterRegistry.timer("etx.mqtt.processing", "message.type", "SPAT").count() > 0);
  }

  @Test
  void testSpatDepositListener_StaleMessage() throws Exception {
    // Load and modify the JSON to have an old timestamp
    OdeSpatData spatData = mapper.readValue(sampleSpatJson, OdeSpatData.class);
    spatData.getMetadata().setOdeReceivedAt("2020-01-01T00:00:00.000Z");
    String staleSpatJson = mapper.writeValueAsString(spatData);

    depositor.spatDepositListener(staleSpatJson);

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(), anyBoolean());
    assertEquals(1.0, meterRegistry.counter("etx.mqtt.stale", "message.type", "SPAT").count());
  }

  @Test
  void testSpatDepositListener_HandlesError() {
    depositor.spatDepositListener("invalid json");

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(), anyBoolean());
  }

  @Test
  void testSpatDepositListener_HandlesMqttFailure() throws Exception {
    // Load and modify the JSON to have a recent timestamp
    OdeSpatData spatData = mapper.readValue(sampleSpatJson, OdeSpatData.class);
    spatData.getMetadata().setOdeReceivedAt(
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
    String recentSpatJson = mapper.writeValueAsString(spatData);

    when(mqttService.publishAsn1Bytes(anyString(), any(), eq(false)))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("MQTT publish failed")));

    depositor.spatDepositListener(recentSpatJson);

    verify(mqttService, atLeastOnce()).publishAsn1Bytes(anyString(), any(), eq(false));
    assert (meterRegistry.timer("etx.mqtt.processing", "message.type", "SPAT").count() > 0);
  }
}
