package us.dot.its.jpo.ode.mec.deposit.etx.mqtt;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.GeoRoutedMsg;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.Position;

/**
 * Builder class for creating ETX MQTT protobuf messages. Handles conversion of message data to
 * protobuf format.
 */
public class EtxMqttProtobufBuilder {

  /**
   * Builds a GeoRoutedMsg protobuf object with the given parameters.
   *
   * @param messageBytes The raw message bytes to include
   * @param timestamp The timestamp for when the message was created/received
   * @param latitude Optional latitude position
   * @param longitude Optional longitude position
   * @return A built GeoRoutedMsg protobuf object
   */
  public static GeoRoutedMsg buildGeoRoutedMsg(byte[] messageBytes, Instant timestamp,
      Double latitude, Double longitude) {

    GeoRoutedMsg.Builder builder = GeoRoutedMsg.newBuilder()
        .setMsgBytes(ByteString.copyFrom(messageBytes)).setTime(Timestamp.newBuilder()
            .setSeconds(timestamp.getEpochSecond()).setNanos(timestamp.getNano()).build());

    // Only set position if both lat and lon are provided
    if (latitude != null && longitude != null) {
      Position position =
          Position.newBuilder().setLatitude(latitude).setLongitude(longitude).build();
      builder.setPosition(position);
    }

    return builder.build();
  }

  /**
   * Builds a GeoRoutedMsg protobuf object without position information.
   *
   * @param messageBytes The raw message bytes to include
   * @param timestamp The timestamp for when the message was created/received
   * @return A built GeoRoutedMsg protobuf object
   */
  public static GeoRoutedMsg buildGeoRoutedMsg(byte[] messageBytes, Instant timestamp) {
    return buildGeoRoutedMsg(messageBytes, timestamp, null, null);
  }
}
