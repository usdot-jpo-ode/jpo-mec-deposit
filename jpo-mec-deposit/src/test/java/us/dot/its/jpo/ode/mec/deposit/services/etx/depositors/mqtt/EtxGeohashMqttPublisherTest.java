package us.dot.its.jpo.ode.mec.deposit.services.etx.depositors.mqtt;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.hsr.geohash.GeoHash;
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
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttBrokerType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.GeoHashRoutedMsg;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.GeoRoutedMsg;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttBrokerTarget;
import us.dot.its.jpo.ode.mec.deposit.services.etx.EtxMqttPublishService;

/**
 * Unit tests for {@link EtxGeohashMqttPublisher}.
 *
 * <p>Input: a Kafka byte payload serialised as {@link GeoHashRoutedMsg}
 * (see {@code scripts/tests/geoHashRoutedMsg.proto}).
 *
 * <p>Expected ETX output (J2735_GR format): a {@link GeoRoutedMsg}
 * (see {@code src/main/proto/geoRoutedMsg.proto}) whose {@code position} field carries the
 * lat/lon coordinates derived from the geohash — matching the wire format produced by every other
 * depositor (BSM, TIM, SPAT, MAP, SDSM, PSM).
 *
 * <p>NMI/AV output (all formats): raw inner ASN.1 bytes only — no protobuf wrapper.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class EtxGeohashMqttPublisherTest {

  // Raw ASN.1 BSM bytes from src/test/resources/sample_messages/sample-ode-bsm.json
  // (lat ≈ 40.566°N, lon ≈ -105.032°W — Northern Colorado).
  private static final byte[] SAMPLE_BSM_BYTES = Hex.decode(
      "001480B8494C4C950CD8CDE6E9651116579F22A424DD78FFFFF00761E4FD7EB7D"
          + "07F7FFF80005F11D1020214C1C0FFC7C016AFF4017A0FF65403B0FD204C20FFC"
          + "CC04F8FE40C420FFE6404CEFE60E9A10133408FCFDE1438103AB4138F00E1EEC1"
          + "048EC160103E237410445C171104E26BC103DC4154305C2C84103B1C1C8F0A82F"
          + "42103F34262D1123198103DAC25FB12034CE10381C259F12038CA103574251B10E"
          + "3B2210324C23AD0F23D8EFFFE0000209340D10000004264BF00");

  // 7-character geohash for approximately the same Northern Colorado location.
  private static final String SAMPLE_GEOHASH = "9xj7kuk";

  // Tolerance for geohash centroid → lat/lon round-trip (precision-7 cell ≈ ±153 m).
  private static final double GEOHASH_LAT_LON_TOLERANCE = 0.005;

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
  private EtxGeohashMqttPublisher publisher;

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

    when(mqttProperties.getPrecision()).thenReturn(7);
    when(mqttProperties.getVendor()).thenReturn("test-vendor");
    when(mqttProperties.getMessageFormat()).thenReturn(EtxMqttMessageFormat.J2735);
    when(mqttProperties.getBrokerType()).thenReturn(EtxMqttBrokerType.ETX);
    when(mqttProperties.isDualPublishEnabled()).thenReturn(false);

    publisher = new EtxGeohashMqttPublisher(mecDepositProperties, etxProperties, mqttProperties,
        mqttService, registry, kafkaTemplate);
  }

  /**
   * Serialises a {@link GeoHashRoutedMsg} as it would arrive on the Kafka topic — inner ASN.1
   * bytes wrapped with a geohash and current timestamp.
   */
  private byte[] buildInputGeoHashRoutedMsg(byte[] innerBytes, String geohash) {
    return GeoHashRoutedMsg.newBuilder()
        .setMsgBytes(ByteString.copyFrom(innerBytes))
        .setGeohash(geohash)
        .setTime(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
        .build()
        .toByteArray();
  }

  // -------------------------------------------------------------------------
  // J2735 (plain ASN.1) format — payload must be raw bytes regardless
  // -------------------------------------------------------------------------

  @Test
  void j2735Format_publishesRawAsn1BytesUnchanged() throws Exception {
    byte[] kafkaPayload = buildInputGeoHashRoutedMsg(SAMPLE_BSM_BYTES, SAMPLE_GEOHASH);
    ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);

    publisher.geohashPublishListener(kafkaPayload);

    verify(mqttService).publishAsn1Bytes(anyString(), payloadCaptor.capture(), eq(false));
    assertArrayEquals(SAMPLE_BSM_BYTES, payloadCaptor.getValue(),
        "J2735 format must publish the raw inner ASN.1 bytes without a protobuf wrapper");
  }

  // -------------------------------------------------------------------------
  // J2735_GR format — ETX payload must be GeoRoutedMsg (src/main/proto/geoRoutedMsg.proto)
  // -------------------------------------------------------------------------

  @Test
  void j2735GrFormat_publishesGeoRoutedMsg_notGeoHashRoutedMsg() throws Exception {
    when(mqttProperties.getMessageFormat()).thenReturn(EtxMqttMessageFormat.J2735_GR);
    byte[] kafkaPayload = buildInputGeoHashRoutedMsg(SAMPLE_BSM_BYTES, SAMPLE_GEOHASH);
    ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);

    publisher.geohashPublishListener(kafkaPayload);

    verify(mqttService).publishAsn1Bytes(anyString(), payloadCaptor.capture(), eq(false));
    // Must parse cleanly as GeoRoutedMsg (src/main/proto/geoRoutedMsg.proto).
    GeoRoutedMsg decoded = GeoRoutedMsg.parseFrom(payloadCaptor.getValue());
    assertNotNull(decoded, "ETX payload must be a valid GeoRoutedMsg protobuf");
  }

  @Test
  void j2735GrFormat_geoRoutedMsg_containsOriginalAsn1Bytes() throws Exception {
    when(mqttProperties.getMessageFormat()).thenReturn(EtxMqttMessageFormat.J2735_GR);
    byte[] kafkaPayload = buildInputGeoHashRoutedMsg(SAMPLE_BSM_BYTES, SAMPLE_GEOHASH);
    ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);

    publisher.geohashPublishListener(kafkaPayload);

    verify(mqttService).publishAsn1Bytes(anyString(), payloadCaptor.capture(), eq(false));
    GeoRoutedMsg decoded = GeoRoutedMsg.parseFrom(payloadCaptor.getValue());
    assertEquals(ByteString.copyFrom(SAMPLE_BSM_BYTES), decoded.getMsgBytes(),
        "GeoRoutedMsg.msgBytes must equal the original inner ASN.1 bytes from the Kafka input");
  }

  @Test
  void j2735GrFormat_geoRoutedMsg_positionDerivedFromGeohash() throws Exception {
    when(mqttProperties.getMessageFormat()).thenReturn(EtxMqttMessageFormat.J2735_GR);
    byte[] kafkaPayload = buildInputGeoHashRoutedMsg(SAMPLE_BSM_BYTES, SAMPLE_GEOHASH);
    ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);

    publisher.geohashPublishListener(kafkaPayload);

    verify(mqttService).publishAsn1Bytes(anyString(), payloadCaptor.capture(), eq(false));
    GeoRoutedMsg decoded = GeoRoutedMsg.parseFrom(payloadCaptor.getValue());

    assertTrue(decoded.hasPosition(),
        "GeoRoutedMsg must carry a Position derived from the input geohash");

    // Verify the position matches the centroid of the geohash cell.
    ch.hsr.geohash.GeoHash gh = GeoHash.fromGeohashString(SAMPLE_GEOHASH);
    double expectedLat = gh.getOriginatingPoint().getLatitude();
    double expectedLon = gh.getOriginatingPoint().getLongitude();

    assertEquals(expectedLat, decoded.getPosition().getLatitude(), GEOHASH_LAT_LON_TOLERANCE,
        "GeoRoutedMsg.position.latitude must be the centroid of the input geohash cell");
    assertEquals(expectedLon, decoded.getPosition().getLongitude(), GEOHASH_LAT_LON_TOLERANCE,
        "GeoRoutedMsg.position.longitude must be the centroid of the input geohash cell");
  }

  @Test
  void j2735GrFormat_geoRoutedMsg_hasDepositTimestamp() throws Exception {
    when(mqttProperties.getMessageFormat()).thenReturn(EtxMqttMessageFormat.J2735_GR);
    byte[] kafkaPayload = buildInputGeoHashRoutedMsg(SAMPLE_BSM_BYTES, SAMPLE_GEOHASH);
    ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);

    long beforeEpochSec = Instant.now().getEpochSecond();
    publisher.geohashPublishListener(kafkaPayload);
    long afterEpochSec = Instant.now().getEpochSecond();

    verify(mqttService).publishAsn1Bytes(anyString(), payloadCaptor.capture(), eq(false));
    GeoRoutedMsg decoded = GeoRoutedMsg.parseFrom(payloadCaptor.getValue());

    assertTrue(decoded.hasTime(), "GeoRoutedMsg must include a deposit timestamp");
    long ts = decoded.getTime().getSeconds();
    assertTrue(ts >= beforeEpochSec && ts <= afterEpochSec,
        "GeoRoutedMsg timestamp must fall within the deposit window");
  }

  @Test
  void j2735GrFormat_innerBytesAreNotDoubleWrapped() throws Exception {
    when(mqttProperties.getMessageFormat()).thenReturn(EtxMqttMessageFormat.J2735_GR);
    byte[] kafkaPayload = buildInputGeoHashRoutedMsg(SAMPLE_BSM_BYTES, SAMPLE_GEOHASH);
    ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);

    publisher.geohashPublishListener(kafkaPayload);

    verify(mqttService).publishAsn1Bytes(anyString(), payloadCaptor.capture(), eq(false));
    GeoRoutedMsg decoded = GeoRoutedMsg.parseFrom(payloadCaptor.getValue());
    assertArrayEquals(SAMPLE_BSM_BYTES, decoded.getMsgBytes().toByteArray(),
        "GeoRoutedMsg.msgBytes must be raw ASN.1 — the publisher must not nest a protobuf "
            + "inside another protobuf");
  }

  // -------------------------------------------------------------------------
  // Topic format
  // -------------------------------------------------------------------------

  @Test
  void etxTopicContainsGeohashSegments() throws Exception {
    byte[] kafkaPayload = buildInputGeoHashRoutedMsg(SAMPLE_BSM_BYTES, SAMPLE_GEOHASH);
    ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);

    publisher.geohashPublishListener(kafkaPayload);

    verify(mqttService).publishAsn1Bytes(topicCaptor.capture(), any(byte[].class), eq(false));
    String topic = topicCaptor.getValue();
    assertTrue(topic.startsWith("vzimp/1/"),
        "ETX topic must start with 'vzimp/1/'; got: " + topic);
    for (char c : SAMPLE_GEOHASH.toCharArray()) {
      assertTrue(topic.contains("/" + c + "/"),
          "ETX topic must contain geohash character '/" + c + "/'; got: " + topic);
    }
  }

  // -------------------------------------------------------------------------
  // Error / edge-case handling
  // -------------------------------------------------------------------------

  @Test
  void malformedInputBytes_doesNotThrow_noMqttPublish() {
    byte[] garbage = {0x00, 0x01, 0x02, 0x03};

    publisher.geohashPublishListener(garbage);

    verify(mqttService, never()).publishAsn1Bytes(anyString(), any(byte[].class), eq(false));
  }

  @Test
  void emptyGeohash_j2735Format_fallsBackToDefaultTopic_publishesRawBytes() throws Exception {
    byte[] kafkaPayload = buildInputGeoHashRoutedMsg(SAMPLE_BSM_BYTES, "");
    ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);

    publisher.geohashPublishListener(kafkaPayload);

    verify(mqttService).publishAsn1Bytes(topicCaptor.capture(), payloadCaptor.capture(), eq(false));
    assertTrue(topicCaptor.getValue().startsWith("vzimp/1/"),
        "Fallback topic must still follow the vzimp/1/ prefix; got: " + topicCaptor.getValue());
    assertArrayEquals(SAMPLE_BSM_BYTES, payloadCaptor.getValue(),
        "J2735 format with empty geohash must publish raw ASN.1 bytes");
  }

  @Test
  void emptyGeohash_j2735GrFormat_fallsBackToDefaultTopic_publishesGeoRoutedMsg()
      throws Exception {
    when(mqttProperties.getMessageFormat()).thenReturn(EtxMqttMessageFormat.J2735_GR);
    byte[] kafkaPayload = buildInputGeoHashRoutedMsg(SAMPLE_BSM_BYTES, "");
    ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);

    publisher.geohashPublishListener(kafkaPayload);

    // With an empty geohash the topic builder falls back to a wildcard default topic;
    // the payload is still a valid GeoRoutedMsg wrapping the inner ASN.1 bytes.
    verify(mqttService).publishAsn1Bytes(topicCaptor.capture(), payloadCaptor.capture(), eq(false));
    assertTrue(topicCaptor.getValue().startsWith("vzimp/1/"),
        "Fallback topic must follow the vzimp/1/ prefix; got: " + topicCaptor.getValue());
    GeoRoutedMsg decoded = GeoRoutedMsg.parseFrom(payloadCaptor.getValue());
    assertEquals(ByteString.copyFrom(SAMPLE_BSM_BYTES), decoded.getMsgBytes(),
        "GeoRoutedMsg.msgBytes must still equal the original ASN.1 bytes for empty-geohash input");
  }

  // -------------------------------------------------------------------------
  // Schema regression: confirm GeoHashRoutedMsg is NOT the output format
  // -------------------------------------------------------------------------

  /**
   * Regression guard: the ETX payload for J2735_GR must be decodable as {@link GeoRoutedMsg}
   * ({@code src/main/proto/geoRoutedMsg.proto}) and must carry a valid {@code Position} with
   * non-zero coordinates — confirming that the geohash-to-lat/lon conversion is applied before
   * serialisation.
   *
   * <p>Previously the publisher incorrectly emitted a {@link GeoHashRoutedMsg} in this slot,
   * whose field-3 geohash string bytes are incompatible with the {@code Position} embedded-message
   * schema and would cause {@code InvalidProtocolBufferException} on the consumer side.
   */
  @Test
  void j2735GrFormat_outputIsGeoRoutedMsgWithNonZeroPosition_notGeoHashRoutedMsg()
      throws Exception {
    when(mqttProperties.getMessageFormat()).thenReturn(EtxMqttMessageFormat.J2735_GR);
    byte[] kafkaPayload = buildInputGeoHashRoutedMsg(SAMPLE_BSM_BYTES, SAMPLE_GEOHASH);
    ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);

    publisher.geohashPublishListener(kafkaPayload);

    verify(mqttService).publishAsn1Bytes(anyString(), payloadCaptor.capture(), eq(false));
    byte[] publishedBytes = payloadCaptor.getValue();

    // Must parse as GeoRoutedMsg without exception.
    GeoRoutedMsg geoRoutedMsg = GeoRoutedMsg.parseFrom(publishedBytes);
    assertTrue(geoRoutedMsg.hasPosition(),
        "Output GeoRoutedMsg must have a Position field populated from the geohash");
    assertFalse(geoRoutedMsg.getPosition().getLatitude() == 0.0
        && geoRoutedMsg.getPosition().getLongitude() == 0.0,
        "Position must not be (0,0) — coordinates must be derived from the geohash centroid");
  }
}
