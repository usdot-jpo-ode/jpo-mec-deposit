package us.dot.its.jpo.ode.mec.deposit.etx.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import us.dot.its.jpo.ode.mec.deposit.GeoRoutedMsg;

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
}
