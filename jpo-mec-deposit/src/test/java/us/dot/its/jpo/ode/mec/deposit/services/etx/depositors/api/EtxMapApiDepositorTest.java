package us.dot.its.jpo.ode.mec.deposit.services.etx.depositors.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import org.springframework.kafka.core.KafkaTemplate;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxPartnerClient;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxTokenManager;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.DistributionType;
import us.dot.its.jpo.ode.model.OdeMapData;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties.MecDepositMetrics;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties.EtxDepositors;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties.ApiDepositorProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties.DepositorProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientSubType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientType;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class EtxMapApiDepositorTest {

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
  private EtxMapApiDepositor depositor;
  private ObjectMapper objectMapper;
  private String sampleMapJson;
  private DistributionType distributionType = DistributionType.TARGETED;

  @BeforeEach
  void setUp() throws Exception {
    // Use SimpleMeterRegistry instead of mocking
    registry = new SimpleMeterRegistry();
    MockitoAnnotations.openMocks(this);
    objectMapper = new ObjectMapper();

    // Configure MecDepositProperties metrics
    MecDepositMetrics metrics = new MecDepositMetrics();
    metrics.setKafkaTopic("test-metrics-topic");
    when(mecDepositProperties.getMetrics()).thenReturn(metrics);

    when(tokenManager.getValidToken()).thenReturn("mock-token");

    // Configure properties using builders
    ApiDepositorProperties apiDepositorProperties =
        ApiDepositorProperties.builder().distributionType(distributionType).build();

    DepositorProperties depositorProperties =
        DepositorProperties.builder().api(apiDepositorProperties).build();

    EtxDepositors depositors =
        EtxDepositors.builder().staleMessageThreshold(5000).map(depositorProperties).build();

    when(etxProperties.getDepositors()).thenReturn(depositors);
    when(etxProperties.getClientType()).thenReturn(EtxClientType.SOFTWARE);
    when(etxProperties.getClientSubType()).thenReturn(EtxClientSubType.APPLICATION);

    depositor = new EtxMapApiDepositor(mecDepositProperties, etxProperties, etxApiClient,
        tokenManager, registry, kafkaTemplate);

    // Load sample MAP JSON from resources
    sampleMapJson = new String(Files.readAllBytes(Paths.get(
        getClass().getClassLoader().getResource("sample_messages/sample-ode-map.json").toURI())));
  }

  @Test
  void testMapDepositListener_Success() throws Exception {
    // Prepare test data
    OdeMapData mapData = objectMapper.readValue(sampleMapJson, OdeMapData.class);
    String currentTimestamp =
        LocalDateTime.now(ZoneOffset.UTC).atZone(ZoneOffset.UTC).toInstant().toString();
    mapData.getMetadata().setOdeReceivedAt(currentTimestamp);

    // Execute
    depositor.mapDepositListener(objectMapper.writeValueAsString(mapData));

    // Verify
    verify(etxApiClient).deposit(eq("mock-token"), eq(mapData.getMetadata().getAsn1()),
        eq(distributionType));
    verify(kafkaTemplate).send(anyString(),
        argThat(metrics -> metrics.contains("\"success\":true")
            && metrics.contains("\"messageType\":\"MAP\"")
            && metrics.contains("\"distributionType\":\"" + distributionType + "\"")));
  }

  @Test
  void testMapDepositListener_StaleMessage() throws Exception {
    // Prepare test data with old timestamp
    OdeMapData mapData = objectMapper.readValue(sampleMapJson, OdeMapData.class);
    String oldTimestamp =
        LocalDateTime.now(ZoneOffset.UTC).minusMinutes(61).format(DateTimeFormatter.ISO_DATE_TIME);
    mapData.getMetadata().setOdeReceivedAt(oldTimestamp);


    // Execute
    depositor.mapDepositListener(objectMapper.writeValueAsString(mapData));

    // Verify no deposit occurred
    verify(etxApiClient, never()).deposit(anyString(), anyString(), any(DistributionType.class));
  }

  @Test
  void testMapDepositListener_Error() throws Exception {
    // Prepare test data
    OdeMapData mapData = objectMapper.readValue(sampleMapJson, OdeMapData.class);
    String currentTimestamp =
        LocalDateTime.now(ZoneOffset.UTC).atZone(ZoneOffset.UTC).toInstant().toString();
    mapData.getMetadata().setOdeReceivedAt(currentTimestamp);

    // Simulate API error
    doThrow(new RuntimeException("API Error")).when(etxApiClient).deposit(anyString(), anyString(),
        any(DistributionType.class));

    // Execute
    depositor.mapDepositListener(objectMapper.writeValueAsString(mapData));

    // Verify error handling
    verify(kafkaTemplate).send(anyString(),
        argThat(metrics -> metrics.contains("\"success\":false")
            && metrics.contains("\"messageType\":\"MAP\"")
            && metrics.contains("\"distributionType\":\"" + distributionType + "\"")
            && metrics.contains("\"errorMessage\":\"API Error\"")));

    // Verify error counter was incremented
    double errorCount =
        registry.get("mec-deposit.etx.api.error").tag("message.type", "MAP").counter().count();
    assert (errorCount > 0);
  }
}
