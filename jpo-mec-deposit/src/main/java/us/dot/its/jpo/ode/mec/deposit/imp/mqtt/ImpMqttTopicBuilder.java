package us.dot.its.jpo.ode.mec.deposit.imp.mqtt;

import java.util.ArrayList;
import java.util.List;
import ch.hsr.geohash.GeoHash;
import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpProperties;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttMessageType;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttRegionalTopic;
import us.dot.its.jpo.ode.mec.deposit.utils.MapRefPointCollector;
import us.dot.its.jpo.ode.model.OdeTimData;
import us.dot.its.jpo.ode.plugin.j2735.J2735IntersectionState;
import us.dot.its.jpo.ode.plugin.j2735.J2735SPAT;
import us.dot.its.jpo.ode.plugin.j2735.OdePosition3D;
import us.dot.its.jpo.ode.plugin.j2735.common.Position3D;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.GeographicalPath;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.TravelerDataFrame;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.TravelerDataFrameList;

/**
 * Utility class for building MQTT topics according to IMP specifications. Handles topic
 * construction for regional messages and geohash-based routing.
 */
@Slf4j
public class ImpMqttTopicBuilder {
  private static final String MQTT_PREFIX = "vzimp";
  private static final String MQTT_SCHEMA_VERSION = "1";
  private static final String MQTT_PUB_WILDCARD = "-";

  /**
   * Constructs a regional MQTT topic string from the given topic components.
   *
   * @param topic The regional topic components
   * @return Formatted MQTT topic string
   */
  public static String getRegionalTopic(ImpMqttRegionalTopic topic) {
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

    // Truncate if longer than 8 chars
    if (geoHash.length() > 8) {
      geoHash = geoHash.substring(0, 8);
    }
    // Pad with wildcards if shorter than 8
    else if (geoHash.length() < 8) {
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
   * @param impProperties IMP configuration properties
   * @return Complete MQTT topic string
   */
  public static String buildRegionalTopic(ImpMqttMessageType messageType, OdePosition3D refPoint,
      int precision, ImpProperties impProperties) {
    String geoHash = getPubGeoHash(refPoint.getLatitude().doubleValue(),
        refPoint.getLongitude().doubleValue(), precision);
    ImpMqttProperties mqttProperties = impProperties.getMqtt();
    ImpMqttRegionalTopic topic = ImpMqttRegionalTopic.builder().mqttGeohash(geoHash)
        .vendorId(mqttProperties.getVendor()).messageFormat(mqttProperties.getMessageFormat())
        .messageType(messageType).clientType(impProperties.getClientType())
        .clientSubType(impProperties.getClientSubType()).build();
    return getRegionalTopic(topic);
  }

  public static String buildRegionalTopic(ImpMqttMessageType messageType, double latitude,
      double longitude, int precision, ImpProperties impProperties) {
    String geoHash = getPubGeoHash(latitude, longitude, precision);
    ImpMqttProperties mqttProperties = impProperties.getMqtt();
    ImpMqttRegionalTopic topic = ImpMqttRegionalTopic.builder().mqttGeohash(geoHash)
        .vendorId(mqttProperties.getVendor()).messageFormat(mqttProperties.getMessageFormat())
        .messageType(messageType).clientType(impProperties.getClientType())
        .clientSubType(impProperties.getClientSubType()).build();
    return getRegionalTopic(topic);
  }

  public static List<String> getTimTopicList(TravelerDataFrameList dataFramesList,
      ImpProperties impProperties) {
    List<String> topicList = new ArrayList<>();
    for (TravelerDataFrame dataFrame : dataFramesList) {
      var regions = dataFrame.getRegions();
      for (GeographicalPath region : regions) {
        Position3D refPoint = region.getAnchor();
        if (refPoint == null) {
          log.warn("No refPoint found for region: {} skipping IMP deposit", region.getName());
          continue;
        }
        // Convert from J2735 integer microdegrees to decimal degrees
        double scale = 10000000.0;
        double latitude = refPoint.getLat().getValue() / scale; // 38.9549122
        double longitude = refPoint.getLong_().getValue() / scale; // -77.1490570
        String topic =
            buildRegionalTopic(ImpMqttMessageType.TIM, latitude, longitude, 7, impProperties);

        topicList.add(topic);
      }
    }
    return topicList;
  }

  public static List<String> getSpatTopicList(J2735SPAT spatMsg, ImpProperties impProperties,
      MapRefPointCollector mapDataCollector) {
    List<String> topicList = new ArrayList<>();
    for (J2735IntersectionState intersection : spatMsg.getIntersectionStateList()
        .getIntersectionStatelist()) {
      String intersectionId = intersection.getId().getId().toString();
      OdePosition3D refPoint = mapDataCollector.getIntersectionRefPoint(intersectionId);
      if (refPoint == null) {
        log.warn("No refPoint found for intersectionId: {} skipping IMP deposit", intersectionId);
        continue;
      }

      String topic = buildRegionalTopic(ImpMqttMessageType.SPAT, refPoint, 7, impProperties);

      topicList.add(topic);
    }
    return topicList;
  }
}
