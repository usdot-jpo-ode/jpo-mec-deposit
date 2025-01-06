package us.dot.its.jpo.ode.mec.deposit.etx.mqtt;

import java.util.HashSet;
import java.util.Set;
import ch.hsr.geohash.GeoHash;
import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.models.mqtt.EtxMqttMessageType;
import us.dot.its.jpo.ode.mec.deposit.etx.models.mqtt.EtxMqttRegionalTopic;
import us.dot.its.jpo.ode.mec.deposit.utils.MapRefPointCollector;
import us.dot.its.jpo.ode.plugin.j2735.J2735IntersectionState;
import us.dot.its.jpo.ode.plugin.j2735.J2735SPAT;
import us.dot.its.jpo.ode.plugin.j2735.OdePosition3D;
import us.dot.its.jpo.ode.plugin.j2735.common.Position3D;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.GeographicalPath;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.TravelerDataFrame;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.TravelerDataFrameList;

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
   * Builds a complete regional MQTT topic string using message type, position, and properties.
   *
   * @param messageType The type of message being published
   * @param refPoint Reference position for geohash calculation
   * @param precision Desired geohash precision
   * @param etxProperties ETX configuration properties
   * @return Complete MQTT topic string
   */
  public static String buildRegionalTopic(EtxMqttMessageType messageType, OdePosition3D refPoint,
      int precision, EtxProperties etxProperties) {
    String geoHash = getPubGeoHash(refPoint.getLatitude().doubleValue(),
        refPoint.getLongitude().doubleValue(), precision);
    EtxMqttProperties mqttProperties = etxProperties.getMqtt();
    EtxMqttRegionalTopic topic = EtxMqttRegionalTopic.builder().mqttGeohash(geoHash)
        .vendorId(mqttProperties.getVendor()).messageFormat(mqttProperties.getMessageFormat())
        .messageType(messageType).clientType(etxProperties.getClientType())
        .clientSubType(etxProperties.getClientSubType()).build();
    return getRegionalTopic(topic);
  }

  public static String buildRegionalTopic(EtxMqttMessageType messageType, double latitude,
      double longitude, int precision, EtxProperties etxProperties) {
    String geoHash = getPubGeoHash(latitude, longitude, precision);
    EtxMqttProperties mqttProperties = etxProperties.getMqtt();
    EtxMqttRegionalTopic topic = EtxMqttRegionalTopic.builder().mqttGeohash(geoHash)
        .vendorId(mqttProperties.getVendor()).messageFormat(mqttProperties.getMessageFormat())
        .messageType(messageType).clientType(etxProperties.getClientType())
        .clientSubType(etxProperties.getClientSubType()).build();
    return getRegionalTopic(topic);
  }

  public static Set<String> getTimTopicList(TravelerDataFrameList dataFramesList,
      EtxProperties etxProperties) {
    Set<String> topicSet = new HashSet<>();
    for (TravelerDataFrame dataFrame : dataFramesList) {
      var regions = dataFrame.getRegions();
      for (GeographicalPath region : regions) {
        Position3D refPoint = region.getAnchor();
        if (refPoint == null) {
          log.warn("No refPoint found for region: {} skipping ETX deposit", region.getName());
          continue;
        }
        double scale = 10000000.0;
        double latitude = refPoint.getLat().getValue() / scale;
        double longitude = refPoint.getLong_().getValue() / scale;
        String topic =
            buildRegionalTopic(EtxMqttMessageType.TIM, latitude, longitude, 7, etxProperties);

        topicSet.add(topic);
      }
    }
    return topicSet;
  }

  public static Set<String> getSpatTopicList(J2735SPAT spatMsg, EtxProperties etxProperties,
      MapRefPointCollector mapDataCollector) {
    Set<String> topicSet = new HashSet<>();
    for (J2735IntersectionState intersection : spatMsg.getIntersectionStateList()
        .getIntersectionStatelist()) {
      String intersectionId = intersection.getId().getId().toString();
      OdePosition3D refPoint = mapDataCollector.getIntersectionRefPoint(intersectionId);
      if (refPoint == null) {
        log.warn("No refPoint found for intersectionId: {} skipping ETX deposit", intersectionId);
        continue;
      }

      String topic = buildRegionalTopic(EtxMqttMessageType.SPAT, refPoint, 7, etxProperties);

      topicSet.add(topic);
    }
    return topicSet;
  }
}
