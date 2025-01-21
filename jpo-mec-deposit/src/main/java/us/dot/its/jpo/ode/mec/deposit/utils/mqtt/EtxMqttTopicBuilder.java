package us.dot.its.jpo.ode.mec.deposit.utils.mqtt;

import ch.hsr.geohash.GeoHash;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientSubType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttRegionalTopic;
import us.dot.its.jpo.ode.mec.deposit.utils.MapRefPointCollector;
import us.dot.its.jpo.ode.mec.deposit.utils.PositionConversionUtil;
import us.dot.its.jpo.ode.plugin.j2735.J2735IntersectionState;
import us.dot.its.jpo.ode.plugin.j2735.J2735SPAT;
import us.dot.its.jpo.ode.plugin.j2735.OdePosition3D;
import us.dot.its.jpo.ode.plugin.j2735.common.Position3D;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.GeographicalPath;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.TravelerDataFrame;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.TravelerDataFrameList;

/**
 * Utility class for building MQTT topics according to ETX specifications. This class handles topic
 * construction for regional messages and geohash-based routing. Topics are constructed in the
 * format: vzimp/1/namespace/geohash/clientType/clientSubType/vendorId/messageFormat/messageType
 *
 * <p>
 * The geohash component is formatted as individual characters separated by forward slashes.
 */
@Slf4j
public class EtxMqttTopicBuilder {
  private static final String MQTT_PREFIX = "vzimp";
  private static final String MQTT_SCHEMA_VERSION = "1";
  private static final String MQTT_PUB_WILDCARD = "-";

  /**
   * Constructs a regional MQTT topic string from the given topic components. The resulting topic
   * follows the format:
   * vzimp/1/namespace/geohash/clientType/clientSubType/vendorId/messageFormat/messageType
   *
   * @param topic The regional topic components encapsulated in an EtxMqttRegionalTopic object
   * @return Formatted MQTT topic string with all components properly separated by forward slashes
   */
  public static String getRegionalTopic(EtxMqttRegionalTopic topic) {
    return String.format("%s/%s/%s/%s/%s/%s/%s/%s/%s", MQTT_PREFIX, MQTT_SCHEMA_VERSION,
        topic.getNamespace(), topic.getMqttGeohash(), topic.getClientType(),
        topic.getClientSubType(), topic.getVendorId(), topic.getMessageFormat(),
        topic.getMessageType());
  }

  /**
   * Formats a geohash string for use in MQTT topic paths by splitting it into individual characters
   * separated by forward slashes. The geohash is processed to ensure it meets the required length
   * and precision requirements.
   *
   * @param geoHash The base32 geohash string to format
   * @param precision The required precision (must be between 6 and 8)
   * @return Formatted geohash string with characters separated by forward slashes
   * @throws IllegalArgumentException if geoHash is null/empty or precision is invalid
   */
  public static String getPubTopicGeoHash(String geoHash, int precision) {
    if (geoHash == null) {
      throw new IllegalArgumentException("geoHash cannot be null");
    }
    if (geoHash.isEmpty()) {
      throw new IllegalArgumentException("geoHash cannot be empty");
    }
    if (precision < 6 || precision > 8) {
      throw new IllegalArgumentException("precision must be between 7 and 8");
    }

    if (geoHash.length() > 8) {
      // Truncate if longer than 8 chars
      geoHash = geoHash.substring(0, 8);
    } else if (geoHash.length() < 8) {
      // Pad with wildcards if shorter than 8
      geoHash = geoHash + MQTT_PUB_WILDCARD.repeat(8 - geoHash.length());
    }

    String topicGeohash = String.join("/", geoHash.split(""));
    return topicGeohash;
  }

  /**
   * Generates a formatted geohash string from latitude/longitude coordinates. The resulting geohash
   * is formatted for use in MQTT topics with characters separated by slashes.
   *
   * @param latitude The latitude coordinate in decimal degrees
   * @param longitude The longitude coordinate in decimal degrees
   * @param precision The required geohash precision (must be between 6 and 8)
   * @return Formatted geohash string for MQTT topic with characters separated by slashes
   */
  public static String getPubGeoHash(double latitude, double longitude, int precision) {
    String geoHash = GeoHash.withCharacterPrecision(latitude, longitude, precision).toBase32();
    return getPubTopicGeoHash(geoHash, precision);
  }

  /**
   * Builds a complete regional topic string for the given message type and coordinates. Combines
   * all necessary components including geohash, client information, and message details.
   *
   * @param messageType The type of message (e.g., TIM, SPAT)
   * @param latitude The latitude coordinate in decimal degrees
   * @param longitude The longitude coordinate in decimal degrees
   * @param precision The geohash precision (must be between 6 and 8)
   * @param mqttVendorId The vendor identifier for the MQTT message
   * @param messageFormat The format of the message content
   * @param clientType The type of client sending the message
   * @param clientSubType The subtype of the client sending the message
   * @return The complete formatted topic string
   */
  public static String buildRegionalTopic(EtxMessageType messageType, double latitude,
      double longitude, int precision, String mqttVendorId, EtxMqttMessageFormat messageFormat,
      EtxClientType clientType, EtxClientSubType clientSubType) {
    String geoHash = getPubGeoHash(latitude, longitude, precision);
    EtxMqttRegionalTopic topic = EtxMqttRegionalTopic.builder().mqttGeohash(geoHash)
        .vendorId(mqttVendorId).messageFormat(messageFormat).messageType(messageType)
        .clientType(clientType).clientSubType(clientSubType).build();
    return getRegionalTopic(topic);
  }

  /**
   * Generates a set of MQTT topics for TIM (Traveler Information Message) messages. Creates topics
   * based on the geographical regions specified in the TIM data frames.
   *
   * @param dataFramesList The list of TIM data frames containing region information
   * @param mqttVendorId The vendor identifier for the MQTT message
   * @param precision The geohash precision to use
   * @param messageFormat The format of the message content
   * @param clientType The type of client sending the message
   * @param clientSubType The subtype of the client sending the message
   * @return Set of topic strings, one for each valid region in the TIM message
   */
  public static Set<String> getTimTopicList(TravelerDataFrameList dataFramesList,
      String mqttVendorId, int precision, EtxMqttMessageFormat messageFormat,
      EtxClientType clientType, EtxClientSubType clientSubType) {
    Set<String> topicSet = new HashSet<>();
    for (TravelerDataFrame dataFrame : dataFramesList) {
      var regions = dataFrame.getRegions();
      for (GeographicalPath region : regions) {
        Position3D refPoint = region.getAnchor();
        if (refPoint == null) {
          log.warn("No refPoint found for region: {} skipping ETX deposit", region.getName());
          continue;
        }
        double latitude = PositionConversionUtil.convertRefPointToLat(refPoint);
        double longitude = PositionConversionUtil.convertRefPointToLon(refPoint);
        String topic = buildRegionalTopic(EtxMessageType.TIM, latitude, longitude, precision,
            mqttVendorId, messageFormat, clientType, clientSubType);

        topicSet.add(topic);
      }
    }
    return topicSet;
  }

  /**
   * Generates a set of MQTT topics for SPAT (Signal Phase and Timing) messages. Creates topics
   * based on the intersection locations referenced in the SPAT message.
   *
   * @param spatMsg The SPAT message containing intersection information
   * @param mapDataCollector The collector containing intersection reference points
   * @param mqttVendorId The vendor identifier for the MQTT message
   * @param precision The geohash precision to use
   * @param messageFormat The format of the message content
   * @param clientType The type of client sending the message
   * @param clientSubType The subtype of the client sending the message
   * @return Set of topic strings, one for each intersection with valid reference points
   */
  public static Set<String> getSpatTopicList(J2735SPAT spatMsg,
      MapRefPointCollector mapDataCollector, String mqttVendorId, int precision,
      EtxMqttMessageFormat messageFormat, EtxClientType clientType,
      EtxClientSubType clientSubType) {
    Set<String> topicSet = new HashSet<>();
    for (J2735IntersectionState intersection : spatMsg.getIntersectionStateList()
        .getIntersectionStatelist()) {
      String intersectionId = intersection.getId().getId().toString();
      OdePosition3D refPoint = mapDataCollector.getIntersectionRefPoint(intersectionId);
      if (refPoint == null) {
        log.warn("No refPoint found for intersectionId: {} skipping ETX deposit", intersectionId);
        continue;
      }

      double latitude = refPoint.getLatitude().doubleValue();
      double longitude = refPoint.getLongitude().doubleValue();

      String topic = buildRegionalTopic(EtxMessageType.SPAT, latitude, longitude, precision,
          mqttVendorId, messageFormat, clientType, clientSubType);

      topicSet.add(topic);
    }
    return topicSet;
  }
}
