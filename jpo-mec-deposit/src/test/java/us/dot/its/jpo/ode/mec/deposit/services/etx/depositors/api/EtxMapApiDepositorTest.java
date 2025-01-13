package us.dot.its.jpo.ode.mec.deposit.services.etx.depositors.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.TestPropertySource;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxPartnerClient;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxTokenManager;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxPartnerApiProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.DistributionType;
import us.dot.its.jpo.ode.mec.deposit.test.EtxDepositTestConfig;
import us.dot.its.jpo.ode.model.OdeMapData;

@SpringBootTest
@Import(EtxDepositTestConfig.class)
@TestPropertySource(locations = "classpath:application.yaml", properties = {
    "mec-deposit.etx.enabled=true", "mec-deposit.etx.depositors.map.api.enabled=true"})
class EtxMapApiDepositorTest {

  private EtxMapApiDepositor depositor;
  private String sampleMapJson;
  private ObjectMapper mapper;

  @Autowired
  private MecDepositProperties mecDepositProperties;

  @Autowired
  private EtxProperties etxProperties;

  @Autowired
  private EtxPartnerApiProperties etxPartnerApiProperties;

  @Autowired
  private MeterRegistry meterRegistry;

  @Mock
  private EtxPartnerClient etxApi;

  @Mock
  private EtxTokenManager tokenManager;

  @Mock
  private KafkaTemplate<String, String> kafkaTemplate;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    mapper = new ObjectMapper();

    when(tokenManager.getValidToken()).thenReturn("mock-token");

    depositor = new EtxMapApiDepositor(mecDepositProperties, etxProperties, etxApi, tokenManager,
        meterRegistry, kafkaTemplate);

    // Load sample MAP JSON from resources
    sampleMapJson = new String(Files.readAllBytes(Paths.get(
        getClass().getClassLoader().getResource("sample_messages/sample-ode-map.json").toURI())));
  }

  @Test
  void testMapDepositListener_SuccessfulDeposit() throws Exception {
    OdeMapData mapData = mapper.readValue(sampleMapJson, OdeMapData.class);
    mapData.getMetadata().setOdeReceivedAt(
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
    String recentMapJson = mapper.writeValueAsString(mapData);

    depositor.mapDepositListener(recentMapJson);

    verify(tokenManager).getValidToken();
    verify(etxApi).deposit(eq("mock-token"), anyString(), eq(DistributionType.TARGETED));
    verify(kafkaTemplate).send(anyString(), anyString());

    assert (meterRegistry.timer("mec-deposit.etx.api.processing", "message.type", "MAP")
        .count() > 0);
  }

  @Test
  void testMapDepositListener_ApiError() throws Exception {
    OdeMapData mapData = mapper.readValue(sampleMapJson, OdeMapData.class);
    mapData.getMetadata().setOdeReceivedAt(
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
    String recentMapJson = mapper.writeValueAsString(mapData);

    // Simulate API error
    doThrow(new RuntimeException("API Error")).when(etxApi).deposit(anyString(), anyString(),
        any());

    depositor.mapDepositListener(recentMapJson);

    verify(tokenManager).getValidToken();
    verify(etxApi).deposit(anyString(), anyString(), any());
    verify(kafkaTemplate).send(anyString(),
        argThat(metricsJson -> metricsJson.contains("\"success\":false")
            && metricsJson.contains("\"errorMessage\":\"API Error\"")));
  }

  @Test
  void testMapDepositListener_TokenError() throws Exception {
    OdeMapData mapData = mapper.readValue(sampleMapJson, OdeMapData.class);
    mapData.getMetadata().setOdeReceivedAt(
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
    String recentMapJson = mapper.writeValueAsString(mapData);

    // Simulate token error
    when(tokenManager.getValidToken()).thenThrow(new RuntimeException("Token Error"));

    depositor.mapDepositListener(recentMapJson);

    verify(tokenManager).getValidToken();
    verify(etxApi, never()).deposit(anyString(), anyString(), any());
    verify(kafkaTemplate).send(anyString(),
        argThat(metricsJson -> metricsJson.contains("\"success\":false")
            && metricsJson.contains("\"errorMessage\":\"Token Error\"")));
  }

  @Test
  void testMapDepositListener_StaleMessage() throws Exception {
    OdeMapData mapData = mapper.readValue(sampleMapJson, OdeMapData.class);
    mapData.getMetadata().setOdeReceivedAt("2020-01-01T00:00:00.000Z");
    String staleMapJson = mapper.writeValueAsString(mapData);

    depositor.mapDepositListener(staleMapJson);

    verify(etxApi, never()).deposit(anyString(), anyString(), any());
    verify(kafkaTemplate, never()).send(anyString(), anyString());
    assertEquals(1.0,
        meterRegistry.counter("mec-deposit.etx.api.stale", "message.type", "MAP").count());
  }

  @Test
  void testMapDepositListener_InvalidJson() {
    depositor.mapDepositListener("invalid json");

    verify(etxApi, never()).deposit(anyString(), anyString(), any());
    verify(kafkaTemplate).send(anyString(),
        argThat(metricsJson -> metricsJson.contains("\"success\":false")));
  }

  @Test
  void testMapDepositListener_NullMessage() {
    depositor.mapDepositListener(null);

    verify(etxApi, never()).deposit(anyString(), anyString(), any());
    verify(kafkaTemplate).send(anyString(),
        argThat(metricsJson -> metricsJson.contains("\"success\":false")));
  }
}
