package us.dot.its.jpo.ode.mec.deposit.etx.depositors.api;

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
import us.dot.its.jpo.ode.mec.deposit.etx.EtxApi;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxTokenManager;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.DistributionType;
import us.dot.its.jpo.ode.mec.deposit.test.etx.depositor.api.config.EtxApiTestConfig;
import us.dot.its.jpo.ode.model.OdeTimData;

@SpringBootTest
@Import(EtxApiTestConfig.class)
@TestPropertySource(locations = "classpath:application.yaml", properties = {
    "mec-deposit.etx.enabled=true", "mec-deposit.etx.depositors.tim.api.enabled=true"})
class EtxTimApiDepositorTest {

  private EtxTimApiDepositor depositor;
  private String sampleTimJson;
  private ObjectMapper mapper;

  @Autowired
  private MecDepositProperties mecDepositProperties;

  @Autowired
  private MeterRegistry meterRegistry;

  @Mock
  private EtxApi etxApi;

  @Mock
  private EtxTokenManager tokenManager;

  @Mock
  private KafkaTemplate<String, String> kafkaTemplate;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    mapper = new ObjectMapper();

    when(tokenManager.getValidToken()).thenReturn("mock-token");

    depositor = new EtxTimApiDepositor(mecDepositProperties, meterRegistry, etxApi, tokenManager,
        kafkaTemplate);

    // Load sample TIM JSON from resources
    sampleTimJson = new String(Files.readAllBytes(Paths.get(
        getClass().getClassLoader().getResource("sample_messages/sample-ode-tim.json").toURI())));
  }

  @Test
  void testTimDepositListener_SuccessfulDeposit() throws Exception {
    OdeTimData timData = mapper.readValue(sampleTimJson, OdeTimData.class);
    timData.getMetadata().setOdeReceivedAt(
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
    String recentTimJson = mapper.writeValueAsString(timData);

    depositor.timDepositListener(recentTimJson);

    verify(tokenManager).getValidToken();
    verify(etxApi).deposit(eq("mock-token"), anyString(), eq(DistributionType.TARGETED));
    verify(kafkaTemplate).send(anyString(), anyString());

    assert (meterRegistry.timer("mec-deposit.etx.api.processing", "message.type", "TIM")
        .count() > 0);
  }

  @Test
  void testTimDepositListener_ApiError() throws Exception {
    OdeTimData timData = mapper.readValue(sampleTimJson, OdeTimData.class);
    timData.getMetadata().setOdeReceivedAt(
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
    String recentTimJson = mapper.writeValueAsString(timData);

    // Simulate API error
    doThrow(new RuntimeException("API Error")).when(etxApi).deposit(anyString(), anyString(),
        any());

    depositor.timDepositListener(recentTimJson);

    verify(tokenManager).getValidToken();
    verify(etxApi).deposit(anyString(), anyString(), any());
    // Verify error metrics were published
    verify(kafkaTemplate).send(anyString(),
        argThat(metricsJson -> metricsJson.contains("\"success\":false")
            && metricsJson.contains("\"errorMessage\":\"API Error\"")));
  }

  @Test
  void testTimDepositListener_TokenError() throws Exception {
    OdeTimData timData = mapper.readValue(sampleTimJson, OdeTimData.class);
    timData.getMetadata().setOdeReceivedAt(
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
    String recentTimJson = mapper.writeValueAsString(timData);

    // Simulate token error
    when(tokenManager.getValidToken()).thenThrow(new RuntimeException("Token Error"));

    depositor.timDepositListener(recentTimJson);

    verify(tokenManager).getValidToken();
    verify(etxApi, never()).deposit(anyString(), anyString(), any());
    // Verify error metrics were published
    verify(kafkaTemplate).send(anyString(),
        argThat(metricsJson -> metricsJson.contains("\"success\":false")
            && metricsJson.contains("\"errorMessage\":\"Token Error\"")));
  }

  @Test
  void testTimDepositListener_StaleMessage() throws Exception {
    OdeTimData timData = mapper.readValue(sampleTimJson, OdeTimData.class);
    timData.getMetadata().setOdeReceivedAt("2020-01-01T00:00:00.000Z");
    String staleTimJson = mapper.writeValueAsString(timData);

    depositor.timDepositListener(staleTimJson);

    verify(etxApi, never()).deposit(anyString(), anyString(), any());
    verify(kafkaTemplate, never()).send(anyString(), anyString()); // No metrics for stale messages
    assertEquals(1.0,
        meterRegistry.counter("mec-deposit.etx.api.stale", "message.type", "TIM").count());
  }

  @Test
  void testTimDepositListener_InvalidJson() {
    depositor.timDepositListener("invalid json");

    verify(etxApi, never()).deposit(anyString(), anyString(), any());
    // Verify error metrics were published
    verify(kafkaTemplate).send(anyString(),
        argThat(metricsJson -> metricsJson.contains("\"success\":false")));
  }

  @Test
  void testTimDepositListener_NullMessage() {
    depositor.timDepositListener(null);

    verify(etxApi, never()).deposit(anyString(), anyString(), any());
    // Verify error metrics were published
    verify(kafkaTemplate).send(anyString(),
        argThat(metricsJson -> metricsJson.contains("\"success\":false")));
  }
}
