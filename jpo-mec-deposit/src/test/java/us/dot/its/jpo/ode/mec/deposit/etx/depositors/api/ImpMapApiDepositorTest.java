package us.dot.its.jpo.ode.mec.deposit.etx.depositors.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import us.dot.its.jpo.ode.mec.deposit.etx.EtxApi;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxTokenManager;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.models.partner.DistributionType;
import us.dot.its.jpo.ode.model.OdeMapData;

class ImpMapApiDepositorTest {

  private ImpMapApiDepositor depositor;
  private MeterRegistry meterRegistry;
  private String sampleMapJson;
  private ObjectMapper mapper;

  @Mock
  private EtxProperties etxProperties;

  @Mock
  private EtxApi etxApi;

  @Mock
  private EtxTokenManager tokenManager;

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
    when(tokenManager.getValidToken()).thenReturn("mock-token");

    depositor = new ImpMapApiDepositor(etxProperties, meterRegistry, etxApi, tokenManager);

    // Load sample MAP JSON from resources
    sampleMapJson = new String(Files.readAllBytes(Paths.get(
        getClass().getClassLoader().getResource("sample_messages/sample-ode-map.json").toURI())));
  }

  @Test
  void testMapDepositListener_SuccessfulDeposit() throws Exception {
    // Load and modify the JSON to have a recent timestamp
    OdeMapData mapData = mapper.readValue(sampleMapJson, OdeMapData.class);
    mapData.getMetadata().setOdeReceivedAt(
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
    String recentMapJson = mapper.writeValueAsString(mapData);

    depositor.mapDepositListener(recentMapJson);

    verify(tokenManager).getValidToken();
    verify(etxApi).deposit(eq("mock-token"), anyString(), eq(DistributionType.TARGETED));
    assert (meterRegistry.timer("etx.api.processing", "message.type", "MAP").count() > 0);
  }

  @Test
  void testMapDepositListener_StaleMessage() throws Exception {
    // Load and modify the JSON to have an old timestamp
    OdeMapData mapData = mapper.readValue(sampleMapJson, OdeMapData.class);
    mapData.getMetadata().setOdeReceivedAt("2020-01-01T00:00:00.000Z");
    String staleMapJson = mapper.writeValueAsString(mapData);

    depositor.mapDepositListener(staleMapJson);

    verify(etxApi, never()).deposit(anyString(), anyString(), any());
    assertEquals(1.0, meterRegistry.counter("etx.api.stale", "message.type", "MAP").count());
  }

  @Test
  void testMapDepositListener_HandlesError() {
    depositor.mapDepositListener("invalid json");

    verify(etxApi, never()).deposit(anyString(), anyString(), any());
  }
}
