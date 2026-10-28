package us.dot.its.jpo.ode.mec.deposit.services.depositors.mqtt;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import org.bouncycastle.util.encoders.Hex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.kafka.core.KafkaTemplate;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties.MecDepositMetrics;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientSubType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttBrokerTarget;
import us.dot.its.jpo.ode.mec.deposit.services.etx.mqtt.EtxMqttPublishService;
import us.dot.its.jpo.ode.rtk_asn1_bridge.models.Coordinate;
import us.dot.its.jpo.ode.rtk_asn1_bridge.models.MsgType;
import us.dot.its.jpo.ode.rtk_asn1_bridge.models.Polygon;
import us.dot.its.jpo.ode.rtk_asn1_bridge.models.PolygonMessage;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PolygonMqttPublisherTest {

  private static final byte[] SAMPLE_BSM_BYTES =
      Hex.decode("001480B8494C4C950CD8CDE6E9651116579F22A424DD78FFFFF00761E4FD7EB7D"
          + "07F7FFF80005F11D1020214C1C0FFC7C016AFF4017A0FF65403B0FD204C20FFC"
          + "CC04F8FE40C420FFE6404CEFE60E9A10133408FCFDE1438103AB4138F00E1EEC1"
          + "048EC160103E237410445C171104E26BC103DC4154305C2C84103B1C1C8F0A82F"
          + "42103F34262D1123198103DAC25FB12034CE10381C259F12038CA103574251B10E"
          + "3B2210324C23AD0F23D8EFFFE0000209340D10000004264BF00");

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

  private MeterRegistry registry;
  private PolygonMqttPublisher publisher;

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();

    MecDepositMetrics metrics = new MecDepositMetrics();
    metrics.setKafkaTopic("test-metrics-topic");
    metrics.setEnabled(true);
    when(mecDepositProperties.getMetrics()).thenReturn(metrics);

    when(etxProperties.isMqttDepositorEnabled(any(MqttBrokerTarget.class), anyString()))
        .thenReturn(true);
    when(etxProperties.getClientType()).thenReturn(EtxClientType.SOFTWARE);
    when(etxProperties.getClientSubType()).thenReturn(EtxClientSubType.APPLICATION);
    when(etxProperties.mqttTopicPrecision(any())).thenReturn(7);
    when(etxProperties.resolveStaleMessageThresholdMs()).thenReturn(5_000);

    when(mqttProperties.getPrecision()).thenReturn(7);
    when(mqttProperties.getVendor()).thenReturn("test-vendor");
    when(mqttProperties.getMessageFormat()).thenReturn(EtxMqttMessageFormat.J2735);
    when(mqttProperties.getBrokerType()).thenReturn(MqttBrokerTarget.ETX);
    when(mqttProperties.isMultiBrokerEnabled()).thenReturn(false);

    publisher = new PolygonMqttPublisher(mecDepositProperties, etxProperties, mqttProperties,
        mqttService, registry, kafkaTemplate);
  }

  private byte[] buildInputPolygonMessage(byte[] innerBytes, MsgType msgType,
      Coordinate coordinate) {
    Polygon polygon = Polygon.newBuilder()
        .addVertices(Coordinate.newBuilder().setLatitude(38.8895).setLongitude(-77.0352).build())
        .addVertices(Coordinate.newBuilder().setLatitude(38.8897).setLongitude(-77.0348).build())
        .addVertices(Coordinate.newBuilder().setLatitude(38.8893).setLongitude(-77.0346).build())
        .build();

    return PolygonMessage.newBuilder().setMsgBytes(ByteString.copyFrom(innerBytes))
        .setMsgType(msgType)
        .setTime(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
        .setCoordinate(coordinate).setPolygon(polygon).build().toByteArray();
  }

  @Test
  void j2735Format_publishesRawAsn1BytesUnchanged() {
    Coordinate coordinate =
        Coordinate.newBuilder().setLatitude(38.8895).setLongitude(-77.0352).build();
    byte[] kafkaPayload = buildInputPolygonMessage(SAMPLE_BSM_BYTES, MsgType.BSM, coordinate);
    ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);

    publisher.polygonPublishListener(kafkaPayload);

    verify(mqttService).publishAsn1Bytes(anyString(), payloadCaptor.capture(), eq(false));
    assertArrayEquals(SAMPLE_BSM_BYTES, payloadCaptor.getValue());
  }

  @Test
  void validPolygonMessage_buildsRegionalTopic() {
    Coordinate coordinate =
        Coordinate.newBuilder().setLatitude(38.8895).setLongitude(-77.0352).build();
    byte[] kafkaPayload = buildInputPolygonMessage(SAMPLE_BSM_BYTES, MsgType.BSM, coordinate);
    ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);

    publisher.polygonPublishListener(kafkaPayload);

    verify(mqttService).publishAsn1Bytes(topicCaptor.capture(), any(byte[].class), eq(false));
    assertTrue(topicCaptor.getValue().startsWith("vzimp/1/"));
  }

  @Test
  void malformedInputBytes_doesNotThrow_noMqttPublish() {
    byte[] garbage = {0x00, 0x01, 0x02, 0x03};

    publisher.polygonPublishListener(garbage);

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(byte[].class), eq(false));
  }

  @Test
  void noRepresentativeCoordinateAndNoVertices_noMqttPublish() {
    byte[] kafkaPayload = PolygonMessage.newBuilder()
        .setMsgBytes(ByteString.copyFrom(SAMPLE_BSM_BYTES)).setMsgType(MsgType.BSM)
        .setTime(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build()).build()
        .toByteArray();

    publisher.polygonPublishListener(kafkaPayload);

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(byte[].class), eq(false));
  }
}
