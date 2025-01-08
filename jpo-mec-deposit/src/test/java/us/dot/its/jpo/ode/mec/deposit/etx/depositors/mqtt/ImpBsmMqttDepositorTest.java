package us.dot.its.jpo.ode.mec.deposit.etx.depositors.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttService;
import us.dot.its.jpo.ode.model.OdeBsmData;


class ImpBsmMqttDepositorTest {

  private ImpBsmMqttDepositor depositor;
  private MeterRegistry meterRegistry;
  private String sampleBsmJson;
  private ObjectMapper mapper;

  @Mock
  private EtxProperties etxProperties;

  @Mock
  private EtxMqttService mqttService;

  @Mock
  private EtxMqttProperties mqttConfig;



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

    depositor = new ImpBsmMqttDepositor(etxProperties, mqttService, meterRegistry);

    // Load sample BSM JSON from resources
    sampleBsmJson = new String(Files.readAllBytes(Paths.get(
        getClass().getClassLoader().getResource("sample_messages/sample-ode-bsm.json").toURI())));
  }

  @Test
  void testBsmDepositListener_SuccessfulDeposit() throws Exception {
    // Load and modify the JSON to have a recent timestamp
    OdeBsmData bsmData = mapper.readValue(sampleBsmJson, OdeBsmData.class);
    bsmData.getMetadata().setOdeReceivedAt(
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
    String recentBsmJson = mapper.writeValueAsString(bsmData);

    depositor.bsmDepositListener(recentBsmJson);

    verify(mqttService, times(1)).publishAsn1Bytes(anyString(), any(), eq(false));
    assert (meterRegistry.timer("etx.mqtt.processing", "message.type", "BSM").count() > 0);
  }

  @Test
  void testBsmDepositListener_StaleMessage() throws Exception {
    // Load and modify the JSON to have an old timestamp
    OdeBsmData bsmData = mapper.readValue(sampleBsmJson, OdeBsmData.class);
    bsmData.getMetadata().setOdeReceivedAt("2020-01-01T00:00:00.000Z");
    String staleBsmJson = mapper.writeValueAsString(bsmData);

    depositor.bsmDepositListener(staleBsmJson);

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(), anyBoolean());
    assertEquals(1.0, meterRegistry.counter("etx.mqtt.stale", "message.type", "BSM").count());
  }

  @Test
  void testBsmDepositListener_HandlesError() {
    depositor.bsmDepositListener("invalid json");

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(), anyBoolean());
  }

  @Test
  void testBsmDepositListener_HandlesMqttFailure() throws Exception {
    // Load and modify the JSON to have a recent timestamp
    OdeBsmData bsmData = mapper.readValue(sampleBsmJson, OdeBsmData.class);
    bsmData.getMetadata().setOdeReceivedAt(
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
    String recentBsmJson = mapper.writeValueAsString(bsmData);

    when(mqttService.publishAsn1Bytes(anyString(), any(), eq(false)))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("MQTT publish failed")));

    depositor.bsmDepositListener(recentBsmJson);

    verify(mqttService, times(1)).publishAsn1Bytes(anyString(), any(), eq(false));
    assert (meterRegistry.timer("etx.mqtt.processing", "message.type", "BSM").count() > 0);
  }
}
