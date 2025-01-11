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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
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
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttService;
import us.dot.its.jpo.ode.mec.deposit.test.etx.depositor.mqtt.config.EtxMqttTestConfig;
import us.dot.its.jpo.ode.model.OdeBsmData;

@SpringBootTest
@Import(EtxMqttTestConfig.class)
@TestPropertySource(locations = "classpath:application.yaml", properties = {
    "mec-deposit.etx.enabled=true", "mec-deposit.etx.depositors.bsm.mqtt.enabled=true"})
class EtxBsmMqttDepositorTest {

  private EtxBsmMqttDepositor depositor;
  private String sampleBsmJson;
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

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    mapper = new ObjectMapper();

    // No need to mock CompletableFuture anymore
    doNothing().when(mqttService).publishAsn1Bytes(anyString(), any(), eq(false));

    depositor = new EtxBsmMqttDepositor(mecDepositProperties, etxProperties, mqttService,
        meterRegistry, kafkaTemplate);

    // Load sample BSM JSON from resources
    sampleBsmJson = new String(Files.readAllBytes(Paths.get(
        getClass().getClassLoader().getResource("sample_messages/sample-ode-bsm.json").toURI())));
  }

  @Test
  void testBsmDepositListener_SuccessfulDeposit() throws Exception {
    OdeBsmData bsmData = mapper.readValue(sampleBsmJson, OdeBsmData.class);
    bsmData.getMetadata().setOdeReceivedAt(
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
    String recentBsmJson = mapper.writeValueAsString(bsmData);

    depositor.bsmDepositListener(recentBsmJson);

    verify(mqttService, times(1)).publishAsn1Bytes(anyString(), any(), eq(false));
    verify(kafkaTemplate).send(anyString(), anyString());
    assert (meterRegistry.timer("mec-deposit.etx.mqtt.processing", "message.type", "BSM")
        .count() > 0);
  }

  @Test
  void testBsmDepositListener_StaleMessage() throws Exception {
    OdeBsmData bsmData = mapper.readValue(sampleBsmJson, OdeBsmData.class);
    bsmData.getMetadata().setOdeReceivedAt("2020-01-01T00:00:00.000Z");
    String staleBsmJson = mapper.writeValueAsString(bsmData);

    depositor.bsmDepositListener(staleBsmJson);

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(), anyBoolean());
    verify(kafkaTemplate, never()).send(anyString(), anyString()); // No metrics for stale messages
    assertEquals(1.0,
        meterRegistry.counter("mec-deposit.etx.mqtt.stale", "message.type", "BSM").count());
  }

  @Test
  void testBsmDepositListener_InvalidJson() {
    depositor.bsmDepositListener("invalid json");

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(), anyBoolean());
    verify(kafkaTemplate).send(anyString(),
        argThat(metricsJson -> metricsJson.contains("\"success\":false")));
  }

  @Test
  void testBsmDepositListener_NullMessage() {
    depositor.bsmDepositListener(null);

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(), anyBoolean());
    verify(kafkaTemplate).send(anyString(),
        argThat(metricsJson -> metricsJson.contains("\"success\":false")));
  }

  @Test
  void testBsmDepositListener_MqttFailure() throws Exception {
    OdeBsmData bsmData = mapper.readValue(sampleBsmJson, OdeBsmData.class);
    bsmData.getMetadata().setOdeReceivedAt(
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
    String recentBsmJson = mapper.writeValueAsString(bsmData);

    doThrow(new RuntimeException("MQTT publish failed")).when(mqttService)
        .publishAsn1Bytes(anyString(), any(), eq(false));

    depositor.bsmDepositListener(recentBsmJson);

    verify(mqttService, times(1)).publishAsn1Bytes(anyString(), any(), eq(false));
    verify(kafkaTemplate).send(anyString(),
        argThat(metricsJson -> metricsJson.contains("\"success\":false")
            && metricsJson.contains("\"errorMessage\":\"MQTT publish failed\"")));
  }
}
