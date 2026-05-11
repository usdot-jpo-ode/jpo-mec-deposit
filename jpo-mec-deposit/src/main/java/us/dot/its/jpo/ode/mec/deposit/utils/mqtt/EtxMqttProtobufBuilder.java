package us.dot.its.jpo.ode.mec.deposit.utils.mqtt;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.GeoRoutedMsg;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.GeoHashRoutedMsg;
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
    // adding static location within ETX requirements
    return buildGeoRoutedMsg(messageBytes, timestamp, null, null);
  }

  /**
   * Builds a GeoHashRoutedMsg protobuf object with the given parameters.
   *
   * @param messageBytes The raw message bytes to include
   * @param timestamp The timestamp for when the message was created/received
   * @param geohash Optional geohash string
   * @return A built GeoHashRoutedMsg protobuf object
   */
  public static GeoHashRoutedMsg buildGeoHashRoutedMsg(byte[] messageBytes, Instant timestamp,
      String geohash) {

    GeoHashRoutedMsg.Builder builder = GeoHashRoutedMsg.newBuilder()
        .setMsgBytes(ByteString.copyFrom(messageBytes)).setTime(Timestamp.newBuilder()
            .setSeconds(timestamp.getEpochSecond()).setNanos(timestamp.getNano()).build());

    // Only set geohash if provided
    if (geohash != null && !geohash.isEmpty()) {
      builder.setGeohash(geohash);
    }

    return builder.build();
  }

  /**
   * Builds a GeoHashRoutedMsg protobuf object without geohash information.
   *
   * @param messageBytes The raw message bytes to include
   * @param timestamp The timestamp for when the message was created/received
   * @return A built GeoHashRoutedMsg protobuf object
   */
  public static GeoHashRoutedMsg buildGeoHashRoutedMsg(byte[] messageBytes, Instant timestamp) {
    return buildGeoHashRoutedMsg(messageBytes, timestamp, null);
  }

  /**
   * ETX regional MQTT body: ODE ASN.1 bytes as-is unless {@link EtxMqttMessageFormat#J2735_GR}, in
   * which case the payload is a serialized {@link GeoRoutedMsg} wrapping those bytes. NMI/AV should
   * publish {@code rawAsn1} directly and not use this method.
   */
  public static byte[] toEtxMqttWirePayload(byte[] rawAsn1, EtxMqttMessageFormat format,
      Instant timestamp, Double latitude, Double longitude) {
    if (format != EtxMqttMessageFormat.J2735_GR) {
      return rawAsn1;
    }
    return buildGeoRoutedMsg(rawAsn1, timestamp, latitude, longitude).toByteArray();
  }

  /**
   * ETX geohash MQTT body: same rules as {@link #toEtxMqttWirePayload} but uses
   * {@link GeoHashRoutedMsg} so geohash context is preserved on the wire.
   */
  public static byte[] toEtxMqttWirePayloadGeoHash(byte[] rawAsn1, EtxMqttMessageFormat format,
      Instant timestamp, String geohash) {
    if (format != EtxMqttMessageFormat.J2735_GR) {
      return rawAsn1;
    }
    return buildGeoHashRoutedMsg(rawAsn1, timestamp, geohash).toByteArray();
  }
}
