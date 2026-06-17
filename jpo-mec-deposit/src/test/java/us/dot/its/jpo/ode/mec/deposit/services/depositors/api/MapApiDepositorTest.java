package us.dot.its.jpo.ode.mec.deposit.services.depositors.api;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.kafka.core.KafkaTemplate;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties.MecDepositMetrics;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties.ApiDepositorProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties.DepositorProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties.EtxMqttBrokerProfile;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties.MqttBrokerDepositors;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties.MqttBrokers;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxPartnerClient;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxTokenManager;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientSubType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientType;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MapApiDepositorTest {

  @Mock
  private MecDepositProperties mecDepositProperties;

  @Mock
  private EtxProperties etxProperties;

  @Mock
  private EtxPartnerClient etxApiClient;

  @Mock
  private EtxTokenManager tokenManager;

  @Mock
  private KafkaTemplate<String, String> kafkaTemplate;

  @Mock
  private Timer timer;

  @Mock
  private Counter counter;

  private MeterRegistry registry;
  private MapApiDepositor depositor;
  private ObjectMapper objectMapper;
  private String sampleMapJson;

  @BeforeEach
  void setUp() throws IOException, URISyntaxException {
    registry = new SimpleMeterRegistry();
    MockitoAnnotations.openMocks(this);
    objectMapper = new ObjectMapper();

    MecDepositMetrics metrics = new MecDepositMetrics();
    metrics.setKafkaTopic("test-metrics-topic");
    metrics.setEnabled(true);
    when(mecDepositProperties.getMetrics()).thenReturn(metrics);

    when(tokenManager.getValidToken()).thenReturn("mock-token");

    ApiDepositorProperties apiDepositorProperties = ApiDepositorProperties.builder().build();
    DepositorProperties depositorProperties =
        DepositorProperties.builder().api(apiDepositorProperties).build();
    MqttBrokerDepositors mbd =
        MqttBrokerDepositors.builder().staleMessageThreshold(5000).map(depositorProperties).build();
    EtxMqttBrokerProfile etxProfile = EtxMqttBrokerProfile.builder().depositors(mbd).build();
    MqttBrokers brokers = MqttBrokers.builder().etx(etxProfile).build();
    when(etxProperties.getMqttBrokers()).thenReturn(brokers);
    when(etxProperties.resolveStaleMessageThresholdMs()).thenReturn(5000);
    when(etxProperties.getClientType()).thenReturn(EtxClientType.SOFTWARE);
    when(etxProperties.getClientSubType()).thenReturn(EtxClientSubType.APPLICATION);

    depositor = new MapApiDepositor(mecDepositProperties, etxProperties, etxApiClient,
        tokenManager, registry, kafkaTemplate);

    sampleMapJson = new String(Files.readAllBytes(Paths.get(
        getClass().getClassLoader().getResource("sample_messages/sample-ode-map.json").toURI())));
  }

  @Test
  void testMapDepositListener_Success() throws JsonProcessingException {
    OdeMessageFrameData mapData = objectMapper.readValue(sampleMapJson, OdeMessageFrameData.class);
    String currentTimestamp =
        LocalDateTime.now(ZoneOffset.UTC).atZone(ZoneOffset.UTC).toInstant().toString();
    mapData.getMetadata().setOdeReceivedAt(currentTimestamp);

    depositor.mapDepositListener(objectMapper.writeValueAsString(mapData));

    verify(etxApiClient).deposit(eq("mock-token"), eq(mapData.getMetadata().getAsn1()));
    verify(kafkaTemplate).send(anyString(), argThat(metrics -> metrics.contains("\"success\":true")
        && metrics.contains("\"messageType\":\"MAP\"")));
  }

  @Test
  void testMapDepositListener_Error() throws JsonProcessingException {
    OdeMessageFrameData mapData = objectMapper.readValue(sampleMapJson, OdeMessageFrameData.class);
    String currentTimestamp =
        LocalDateTime.now(ZoneOffset.UTC).atZone(ZoneOffset.UTC).toInstant().toString();
    mapData.getMetadata().setOdeReceivedAt(currentTimestamp);

    doThrow(new RuntimeException("API Error")).when(etxApiClient).deposit(anyString(), anyString());

    depositor.mapDepositListener(objectMapper.writeValueAsString(mapData));

    verify(kafkaTemplate).send(anyString(),
        argThat(metrics -> metrics.contains("\"success\":false")
            && metrics.contains("\"messageType\":\"MAP\"")
            && metrics.contains("\"errorMessage\":\"API Error\"")));

    double errorCount =
        registry.get("mec-deposit.etx.api.error").tag("message.type", "MAP").counter().count();
    assert (errorCount > 0);
  }
}
