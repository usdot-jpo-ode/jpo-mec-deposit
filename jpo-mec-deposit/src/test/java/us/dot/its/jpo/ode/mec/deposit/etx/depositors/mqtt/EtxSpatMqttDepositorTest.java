package us.dot.its.jpo.ode.mec.deposit.etx.depositors.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.util.ReflectionTestUtils;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttService;
import us.dot.its.jpo.ode.mec.deposit.test.etx.depositor.mqtt.config.EtxMqttTestConfig;
import us.dot.its.jpo.ode.mec.deposit.utils.MapRefPointCollector;
import us.dot.its.jpo.ode.model.OdeSpatData;
import us.dot.its.jpo.ode.plugin.j2735.OdePosition3D;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;

@SpringBootTest
@Import(EtxMqttTestConfig.class)
@TestPropertySource(locations = "classpath:application.yaml", properties = {
    "mec-deposit.etx.enabled=true", "mec-deposit.etx.depositors.spat.mqtt.enabled=true"})
class EtxSpatMqttDepositorTest {

  private EtxSpatMqttDepositor depositor;
  private String sampleSpatJson;
  private ObjectMapper mapper;

  @Autowired
  private MecDepositProperties mecDepositProperties;

  @Autowired
  private MeterRegistry meterRegistry;

  @Mock
  private EtxProperties etxProperties;

  @Mock
  private EtxMqttService mqttService;

  @Mock
  private KafkaTemplate<String, String> kafkaTemplate;

  @Mock
  private MapRefPointCollector mapDataCollector;

  @Mock
  private EtxMqttProperties mqttProperties;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    mapper = new ObjectMapper();

    // Setup mock properties
    when(etxProperties.getMqtt()).thenReturn(mqttProperties);
    when(mqttProperties.getStaleMessageThreshold()).thenReturn(5000);

    doNothing().when(mqttService).publishAsn1Bytes(anyString(), any(), eq(false));

    depositor =
        new EtxSpatMqttDepositor(mecDepositProperties, mqttService, meterRegistry, kafkaTemplate);

    // Inject the mock mapDataCollector
    ReflectionTestUtils.setField(depositor, "mapDataCollector", mapDataCollector);

    // Mock the topic list response
    when(mapDataCollector.getIntersectionRefPoint(anyString())).thenReturn(new OdePosition3D(
        BigDecimal.valueOf(42.0), BigDecimal.valueOf(-83.0), BigDecimal.valueOf(10.0)));

    // Load sample SPAT JSON from resources
    sampleSpatJson = new String(Files.readAllBytes(Paths.get(
        getClass().getClassLoader().getResource("sample_messages/sample-ode-spat.json").toURI())));
  }

  @Test
  void testSpatDepositListener_SuccessfulDeposit() throws Exception {
    OdeSpatData spatData = mapper.readValue(sampleSpatJson, OdeSpatData.class);
    spatData.getMetadata().setOdeReceivedAt(
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
    String recentSpatJson = mapper.writeValueAsString(spatData);

    depositor.spatDepositListener(recentSpatJson);

    verify(mqttService, times(1)).publishAsn1Bytes(anyString(), any(), eq(false));
    verify(kafkaTemplate).send(anyString(), anyString());
    verify(mapDataCollector).getIntersectionRefPoint(anyString());
    assert (meterRegistry.timer("mec-deposit.etx.mqtt.processing", "message.type", "SPAT")
        .count() > 0);
  }

  @Test
  void testSpatDepositListener_StaleMessage() throws Exception {
    OdeSpatData spatData = mapper.readValue(sampleSpatJson, OdeSpatData.class);
    spatData.getMetadata().setOdeReceivedAt("2020-01-01T00:00:00.000Z");
    String staleSpatJson = mapper.writeValueAsString(spatData);

    depositor.spatDepositListener(staleSpatJson);

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(), anyBoolean());
    verify(kafkaTemplate, never()).send(anyString(), anyString());
    assertEquals(1.0,
        meterRegistry.counter("mec-deposit.etx.mqtt.stale", "message.type", "SPAT").count());
  }

  @Test
  void testSpatDepositListener_InvalidJson() {
    depositor.spatDepositListener("invalid json");

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(), anyBoolean());
    verify(kafkaTemplate).send(anyString(),
        argThat(metricsJson -> metricsJson.contains("\"success\":false")));
  }

  @Test
  void testSpatDepositListener_NullMessage() {
    depositor.spatDepositListener(null);

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(), anyBoolean());
    verify(kafkaTemplate).send(anyString(),
        argThat(metricsJson -> metricsJson.contains("\"success\":false")));
  }

  @Test
  void testSpatDepositListener_MqttFailure() throws Exception {
    OdeSpatData spatData = mapper.readValue(sampleSpatJson, OdeSpatData.class);
    spatData.getMetadata().setOdeReceivedAt(
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
    String recentSpatJson = mapper.writeValueAsString(spatData);

    doThrow(new RuntimeException("MQTT publish failed")).when(mqttService)
        .publishAsn1Bytes(anyString(), any(), eq(false));

    depositor.spatDepositListener(recentSpatJson);

    verify(mqttService, times(1)).publishAsn1Bytes(anyString(), any(), eq(false));
    verify(kafkaTemplate).send(anyString(),
        argThat(metricsJson -> metricsJson.contains("\"success\":false")
            && metricsJson.contains("\"errorMessage\":\"MQTT publish failed\"")));
  }
}
