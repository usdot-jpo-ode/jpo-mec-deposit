package us.dot.its.jpo.ode.mec.deposit.services.depositors.mqtt;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
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
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.kafka.core.KafkaTemplate;
import us.dot.its.jpo.asn.j2735.r2024.TravelerInformation.TravelerDataFrameList;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties.MecDepositMetrics;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.PartnerClient;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.PartnerTokenManager;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientSubType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttBrokerTarget;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.GeofencePreviewResponse;
import us.dot.its.jpo.ode.mec.deposit.services.etx.mqtt.EtxBrokerPublisher;
import us.dot.its.jpo.ode.mec.deposit.services.etx.mqtt.EtxMqttPublishService;
import us.dot.its.jpo.ode.mec.deposit.services.mqtt.MultiBrokerPublishService;
import us.dot.its.jpo.ode.mec.deposit.utils.MapRefPointCollector;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttTopicBuilder;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TimMqttDepositorTest {

  @Mock
  private MecDepositProperties mecDepositProperties;

  @Mock
  private EtxProperties etxProperties;

  @Mock
  private EtxMqttProperties mqttProperties;

  @Mock
  private EtxMqttPublishService mqttService;

  @Mock
  private KafkaTemplate<String, String> kafkaTemplate;

  @Mock
  private Timer timer;

  @Mock
  private Counter counter;

  @Mock
  private MapRefPointCollector mapDataCollector;

  @Mock
  private PartnerClient partnerClient;

  @Mock
  private PartnerTokenManager tokenManager;

  private MeterRegistry registry;
  private TimMqttDepositor depositor;
  private ObjectMapper objectMapper;
  private String sampleTimJson;
  private MockedStatic<EtxMqttTopicBuilder> mockedTopicBuilder;

  @BeforeEach
  void setUp() throws IOException, URISyntaxException {
    registry = new SimpleMeterRegistry();

    MecDepositMetrics metrics = new MecDepositMetrics();
    metrics.setKafkaTopic("test-metrics-topic");
    metrics.setEnabled(true);
    when(mecDepositProperties.getMetrics()).thenReturn(metrics);

    int staleMessageThreshold = 5000;
    when(etxProperties.resolveStaleMessageThresholdMs()).thenReturn(staleMessageThreshold);
    when(etxProperties.isMqttDepositorEnabled(any(), anyString())).thenReturn(true);
    when(etxProperties.nmiMqttTopicPrecision()).thenReturn(7);
    when(etxProperties.mqttTopicPrecision(any())).thenReturn(7);
    when(etxProperties.isTimMqttGeofencePreviewEnabled()).thenReturn(false);
    when(etxProperties.getClientType()).thenReturn(EtxClientType.SOFTWARE);
    when(etxProperties.getClientSubType()).thenReturn(EtxClientSubType.APPLICATION);

    when(mqttProperties.getPrecision()).thenReturn(7);
    when(mqttProperties.getVendor()).thenReturn("test-vendor");
    when(mqttProperties.getMessageFormat()).thenReturn(EtxMqttMessageFormat.J2735);

    depositor = new TimMqttDepositor(mecDepositProperties, etxProperties, mqttProperties,
        mqttService, registry, kafkaTemplate);
    objectMapper = new ObjectMapper();

    sampleTimJson = new String(Files.readAllBytes(Paths.get(
        getClass().getClassLoader().getResource("sample_messages/sample-ode-tim.json").toURI())));

    mockedTopicBuilder = Mockito.mockStatic(EtxMqttTopicBuilder.class);

    mockedTopicBuilder
        .when(() -> EtxMqttTopicBuilder.getTimTopicList(any(TravelerDataFrameList.class),
            anyString(), anyInt(), any(EtxMqttMessageFormat.class), eq(EtxClientType.SOFTWARE),
            eq(EtxClientSubType.APPLICATION)))
        .thenReturn(Set.of("test-topic"));
  }

  @AfterEach
  void tearDown() {
    if (mockedTopicBuilder != null) {
      mockedTopicBuilder.close();
    }
  }

  private String createTimTestMessage(String timestamp) throws JsonProcessingException {
    OdeMessageFrameData timData = objectMapper.readValue(sampleTimJson, OdeMessageFrameData.class);
    timData.getMetadata().setOdeReceivedAt(timestamp);
    return objectMapper.writeValueAsString(timData);
  }

  @Test
  void testTimDepositListener() throws JsonProcessingException {
    String currentTimestamp =
        LocalDateTime.now(ZoneOffset.UTC).atZone(ZoneOffset.UTC).toInstant().toString();
    String message = createTimTestMessage(currentTimestamp);

    when(mqttProperties.getMessageFormat()).thenReturn(EtxMqttMessageFormat.J2735);

    depositor.timDepositListener(message);

    verify(mqttService).publishAsn1Bytes(eq("test-topic"), any(byte[].class), eq(false));
    verify(kafkaTemplate).send(eq("test-metrics-topic"), anyString());
  }

  @Test
  void testTimDepositListenerWithGeoRoutedFormat() throws JsonProcessingException {
    String currentTimestamp =
        LocalDateTime.now(ZoneOffset.UTC).atZone(ZoneOffset.UTC).toInstant().toString();
    String message = createTimTestMessage(currentTimestamp);

    when(mqttProperties.getMessageFormat()).thenReturn(EtxMqttMessageFormat.J2735_GR);

    depositor.timDepositListener(message);

    verify(mqttService).publishAsn1Bytes(anyString(), any(byte[].class), eq(false));
    verify(kafkaTemplate).send(eq("test-metrics-topic"), anyString());
  }

  @Test
  void testTimDepositListener_StaleMessage() throws JsonProcessingException {
    String message = createTimTestMessage("2020-01-01T00:00:00.000Z");

    depositor.timDepositListener(message);

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(byte[].class), eq(false));

    double staleCount =
        registry.get("mec-deposit.etx.mqtt.stale").tag("message.type", "TIM").counter().count();
    assert (staleCount > 0);
  }

  @Test
  void testTimDepositListener_GeofencePreviewTopics() throws Exception {
    when(etxProperties.isTimMqttGeofencePreviewEnabled()).thenReturn(true);
    when(tokenManager.getValidToken()).thenReturn("token");
    when(partnerClient.previewGeofence(eq("token"), anyString())).thenReturn(
        GeofencePreviewResponse.builder().geohashes(List.of("dpsb2yq", "dpsb3n2")).build());

    mockedTopicBuilder
        .when(() -> EtxMqttTopicBuilder.getTimTopicListFromGeohashes(any(), anyString(), anyInt(),
            any(EtxMqttMessageFormat.class), eq(EtxClientType.SOFTWARE),
            eq(EtxClientSubType.APPLICATION)))
        .thenReturn(Set.of("preview-etx-topic-1", "preview-etx-topic-2"));

    String currentTimestamp =
        LocalDateTime.now(ZoneOffset.UTC).atZone(ZoneOffset.UTC).toInstant().toString();
    depositor =
        new TimMqttDepositor(mecDepositProperties, etxProperties, mqttProperties, mqttService,
            registry,
            new MultiBrokerPublishService(List.of(new EtxBrokerPublisher(mqttService)),
                Set.of(MqttBrokerTarget.ETX), registry),
            kafkaTemplate, partnerClient, tokenManager);
    depositor.timDepositListener(createTimTestMessage(currentTimestamp));

    verify(partnerClient).previewGeofence(eq("token"), anyString());
    verify(mqttService).publishAsn1Bytes(eq("preview-etx-topic-1"), any(byte[].class), eq(false));
    verify(mqttService).publishAsn1Bytes(eq("preview-etx-topic-2"), any(byte[].class), eq(false));
    mockedTopicBuilder
        .verify(() -> EtxMqttTopicBuilder.getTimTopicList(any(TravelerDataFrameList.class),
            anyString(), anyInt(), any(EtxMqttMessageFormat.class), eq(EtxClientType.SOFTWARE),
            eq(EtxClientSubType.APPLICATION)), never());
  }

  @Test
  void testTimDepositListener_HandlesException() throws JsonProcessingException {
    String currentTimestamp =
        LocalDateTime.now(ZoneOffset.UTC).atZone(ZoneOffset.UTC).toInstant().toString();
    String message = createTimTestMessage(currentTimestamp);

    doThrow(new RuntimeException("MQTT publish failed")).when(mqttService)
        .publishAsn1Bytes(anyString(), any(byte[].class), eq(false));

    depositor.timDepositListener(message);

    verify(kafkaTemplate).send(eq("test-metrics-topic"),
        argThat(metricsJson -> metricsJson.contains("\"success\":false")
            && metricsJson.contains("\"errorMessage\":\"MQTT publish failed\"")));

    double errorCount =
        registry.get("mec-deposit.etx.mqtt.error").tag("message.type", "TIM").counter().count();
    assert (errorCount > 0);
  }
}
