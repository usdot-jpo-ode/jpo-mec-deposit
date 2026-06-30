package us.dot.its.jpo.ode.mec.deposit.utils.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.GeoHashRoutedMsg;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.GeoRoutedMsg;

class EtxMqttProtobufBuilderTest {

  @Test
  void testBuildGeoRoutedMsgWithPosition() {
    // Setup
    byte[] messageBytes = "test message".getBytes();
    Instant timestamp = Instant.now();
    Double latitude = 40.7128;
    Double longitude = -74.0060;

    // Execute
    GeoRoutedMsg result =
        EtxMqttProtobufBuilder.buildGeoRoutedMsg(messageBytes, timestamp, latitude, longitude);

    // Verify
    assertNotNull(result);
    assertEquals(ByteString.copyFrom(messageBytes), result.getMsgBytes());
    assertEquals(timestamp.getEpochSecond(), result.getTime().getSeconds());
    assertEquals(timestamp.getNano(), result.getTime().getNanos());
    assertTrue(result.hasPosition());
    assertEquals(latitude, result.getPosition().getLatitude());
    assertEquals(longitude, result.getPosition().getLongitude());
  }

  @Test
  void testBuildGeoRoutedMsgWithoutPosition() {
    // Setup
    byte[] messageBytes = "test message".getBytes();
    Instant timestamp = Instant.now();

    // Execute
    GeoRoutedMsg result = EtxMqttProtobufBuilder.buildGeoRoutedMsg(messageBytes, timestamp);

    // Verify
    assertNotNull(result);
    assertEquals(ByteString.copyFrom(messageBytes), result.getMsgBytes());
    assertEquals(timestamp.getEpochSecond(), result.getTime().getSeconds());
    assertEquals(timestamp.getNano(), result.getTime().getNanos());
    assertFalse(result.hasPosition());
  }

  @Test
  void testBuildGeoRoutedMsgWithPartialPosition() {
    // Setup
    byte[] messageBytes = "test message".getBytes();
    Instant timestamp = Instant.now();
    Double latitude = 40.7128;
    Double longitude = null;

    // Execute
    GeoRoutedMsg result =
        EtxMqttProtobufBuilder.buildGeoRoutedMsg(messageBytes, timestamp, latitude, longitude);

    // Verify
    assertNotNull(result);
    assertEquals(ByteString.copyFrom(messageBytes), result.getMsgBytes());
    assertEquals(timestamp.getEpochSecond(), result.getTime().getSeconds());
    assertEquals(timestamp.getNano(), result.getTime().getNanos());
    assertFalse(result.hasPosition());
  }

  @Test
  void toEtxMqttWirePayload_plainJ2735_returnsRawBytes() {
    byte[] raw = {0x01, 0x02, 0x03};
    Instant ts = Instant.parse("2024-01-01T12:00:00Z");
    byte[] out = EtxMqttProtobufBuilder.toEtxMqttWirePayload(raw,
        EtxMqttMessageFormat.J2735, ts, 1.0, 2.0);
    assertSame(raw, out);
  }

  @Test
  void toEtxMqttWirePayload_j2735Gr_wrapsAsGeoRoutedMsg() throws Exception {
    byte[] raw = {0x01, 0x02};
    Instant ts = Instant.parse("2024-01-01T12:00:00Z");
    byte[] out = EtxMqttProtobufBuilder.toEtxMqttWirePayload(raw,
        EtxMqttMessageFormat.J2735_GR, ts, 40.0, -75.0);
    GeoRoutedMsg parsed = GeoRoutedMsg.parseFrom(out);
    assertEquals(ByteString.copyFrom(raw), parsed.getMsgBytes());
    assertTrue(parsed.hasPosition());
  }

  @Test
  void toEtxMqttWirePayloadGeoHash_j2735Gr_preservesGeohash() throws Exception {
    byte[] raw = {0x0a};
    Instant ts = Instant.parse("2024-01-01T12:00:00Z");
    byte[] out = EtxMqttProtobufBuilder.toEtxMqttWirePayloadGeoHash(raw,
        EtxMqttMessageFormat.J2735_GR, ts, "dr5ru");
    GeoHashRoutedMsg parsed = GeoHashRoutedMsg.parseFrom(out);
    assertEquals(ByteString.copyFrom(raw), parsed.getMsgBytes());
    assertEquals("dr5ru", parsed.getGeohash());
  }
}
