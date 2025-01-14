package us.dot.its.jpo.ode.mec.deposit.utils.mqtt;

import ch.hsr.geohash.GeoHash;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientSubType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttRegionalTopic;
import us.dot.its.jpo.ode.mec.deposit.utils.MapRefPointCollector;
import us.dot.its.jpo.ode.plugin.j2735.J2735IntersectionState;
import us.dot.its.jpo.ode.plugin.j2735.J2735SPAT;
import us.dot.its.jpo.ode.plugin.j2735.OdePosition3D;
import us.dot.its.jpo.ode.plugin.j2735.common.Position3D;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.GeographicalPath;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.TravelerDataFrame;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.TravelerDataFrameList;
import us.dot.its.jpo.ode.mec.deposit.utils.PositionConversionUtil;

/**
 * Utility class for building MQTT topics according to ETX specifications. Handles topic
 * construction for regional messages and geohash-based routing.
 */
@Slf4j
public class EtxMqttTopicBuilder {
  private static final String MQTT_PREFIX = "vzimp";
  private static final String MQTT_SCHEMA_VERSION = "1";
  private static final String MQTT_PUB_WILDCARD = "-";

  /**
   * Constructs a regional MQTT topic string from the given topic components.
   *
   * @param topic The regional topic components
   * @return Formatted MQTT topic string
   */
  public static String getRegionalTopic(EtxMqttRegionalTopic topic) {
    return String.format("%s/%s/%s/%s/%s/%s/%s/%s/%s", MQTT_PREFIX, MQTT_SCHEMA_VERSION,
        topic.getNamespace(), topic.getMqttGeohash(), topic.getClientType(),
        topic.getClientSubType(), topic.getVendorId(), topic.getMessageFormat(),
        topic.getMessageType());
  }

  /**
   * Formats a geohash string for use in MQTT topic paths.
   *
   * @param geoHash The geohash to format
   * @param precision The required precision (6-8)
   * @return Formatted geohash string for MQTT topic
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
   * Generates a formatted geohash string from latitude/longitude coordinates.
   *
   * @param latitude The latitude coordinate
   * @param longitude The longitude coordinate
   * @param precision The required geohash precision
   * @return Formatted geohash string for MQTT topic
   */
  public static String getPubGeoHash(double latitude, double longitude, int precision) {
    String geoHash = GeoHash.withCharacterPrecision(latitude, longitude, precision).toBase32();
    return getPubTopicGeoHash(geoHash, precision);
  }

  /**
   * Builds a regional topic for the given message type and coordinates.
   *
   * @param messageType The type of message
   * @param latitude The latitude coordinate
   * @param longitude The longitude coordinate
   * @param precision The geohash precision
   * @param etxProperties The ETX configuration properties
   * @return The formatted topic string
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
   * Gets a list of topics for TIM messages.
   *
   * @param dataFramesList The TIM data frames
   * @param etxProperties The ETX configuration properties
   * @return Set of topic strings
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
   * Gets a list of topics for SPAT messages.
   *
   * @param spatMsg The SPAT message
   * @param etxProperties The ETX configuration properties
   * @param mapDataCollector The map reference point collector
   * @return Set of topic strings
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
