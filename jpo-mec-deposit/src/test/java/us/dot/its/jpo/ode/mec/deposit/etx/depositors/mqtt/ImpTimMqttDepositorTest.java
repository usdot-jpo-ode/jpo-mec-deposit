package us.dot.its.jpo.ode.mec.deposit.etx.depositors.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
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
import us.dot.its.jpo.ode.model.OdeTimData;

class ImpTimMqttDepositorTest {

  private ImpTimMqttDepositor depositor;
  private MeterRegistry meterRegistry;
  private String sampleTimJson;
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

    depositor = new ImpTimMqttDepositor(etxProperties, mqttService, meterRegistry);

    // Load sample TIM JSON from resources
    sampleTimJson = new String(Files.readAllBytes(Paths.get(
        getClass().getClassLoader().getResource("sample_messages/sample-ode-tim.json").toURI())));
  }

  @Test
  void testTimDepositListener_SuccessfulDeposit() throws Exception {
    // Load and modify the JSON to have a recent timestamp
    OdeTimData timData = mapper.readValue(sampleTimJson, OdeTimData.class);
    timData.getMetadata().setOdeReceivedAt(
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
    String recentTimJson = mapper.writeValueAsString(timData);

    depositor.timDepositListener(recentTimJson);

    verify(mqttService, atLeastOnce()).publishAsn1Bytes(anyString(), any(), eq(false));
    assert (meterRegistry.timer("etx.mqtt.processing", "message.type", "TIM").count() > 0);
  }

  @Test
  void testTimDepositListener_StaleMessage() throws Exception {
    // Load and modify the JSON to have an old timestamp
    OdeTimData timData = mapper.readValue(sampleTimJson, OdeTimData.class);
    timData.getMetadata().setOdeReceivedAt("2020-01-01T00:00:00.000Z");
    String staleTimJson = mapper.writeValueAsString(timData);

    depositor.timDepositListener(staleTimJson);

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(), anyBoolean());
    assertEquals(1.0, meterRegistry.counter("etx.mqtt.stale", "message.type", "TIM").count());
  }

  @Test
  void testTimDepositListener_HandlesError() {
    depositor.timDepositListener("invalid json");

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(), anyBoolean());
  }

  @Test
  void testTimDepositListener_HandlesMqttFailure() throws Exception {
    // Load and modify the JSON to have a recent timestamp
    OdeTimData timData = mapper.readValue(sampleTimJson, OdeTimData.class);
    timData.getMetadata().setOdeReceivedAt(
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
    String recentTimJson = mapper.writeValueAsString(timData);

    when(mqttService.publishAsn1Bytes(anyString(), any(), eq(false)))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("MQTT publish failed")));

    depositor.timDepositListener(recentTimJson);

    verify(mqttService, atLeastOnce()).publishAsn1Bytes(anyString(), any(), eq(false));
    assert (meterRegistry.timer("etx.mqtt.processing", "message.type", "TIM").count() > 0);
  }
}
