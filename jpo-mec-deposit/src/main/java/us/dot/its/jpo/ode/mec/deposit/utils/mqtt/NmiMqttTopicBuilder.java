package us.dot.its.jpo.ode.mec.deposit.utils.mqtt;

import java.util.HashSet;
import java.util.Set;
import us.dot.its.jpo.asn.j2735.r2024.Common.Position3D;
import us.dot.its.jpo.asn.j2735.r2024.MapData.IntersectionGeometry;
import us.dot.its.jpo.asn.j2735.r2024.MapData.MapData;
import us.dot.its.jpo.asn.j2735.r2024.SPAT.IntersectionState;
import us.dot.its.jpo.asn.j2735.r2024.SPAT.SPAT;
import us.dot.its.jpo.asn.j2735.r2024.TravelerInformation.GeographicalPath;
import us.dot.its.jpo.asn.j2735.r2024.TravelerInformation.TravelerDataFrame;
import us.dot.its.jpo.asn.j2735.r2024.TravelerInformation.TravelerDataFrameList;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;
import us.dot.its.jpo.ode.mec.deposit.utils.MapRefPointCollector;
import us.dot.its.jpo.ode.mec.deposit.utils.MessageTypeDetector;
import us.dot.its.jpo.ode.mec.deposit.utils.PositionConversionUtil;

/**
 * Utility for building NMI- and AV-compliant MQTT topics (same hierarchical naming).
 */
public final class NmiMqttTopicBuilder {

  private static final String NMI_PREFIX = "v1/g32";

  private NmiMqttTopicBuilder() {}

  /**
   * Builds NMI topic using geohash hierarchy and DSRC message ID topic suffix.
   *
   * @param geohash Geohash string (typically 7 chars)
   * @param precision Geohash precision to apply
   * @param dsrcMsgId Message DSRCmsgID in decimal form
   * @return NMI MQTT topic
   */
  public static String buildTopicFromGeohash(String geohash, int precision, String dsrcMsgId) {
    if (dsrcMsgId == null || dsrcMsgId.isBlank()) {
      throw new IllegalArgumentException("dsrcMsgId cannot be null or blank");
    }
    String formattedGeohash =
        EtxMqttTopicBuilder.getPubTopicGeoHash(geohash, precision).replace("/-", "");
    return NMI_PREFIX + "/" + formattedGeohash + "/" + dsrcMsgId;
  }

  /**
   * Builds NMI topic from coordinates and message type.
   */
  public static String buildTopicFromCoordinates(double latitude, double longitude, int precision,
      EtxMessageType messageType) {
    String geohash =
        EtxMqttTopicBuilder.getPubGeoHash(latitude, longitude, precision).replace("/-", "");
    String dsrcMsgId = MessageTypeDetector.getDsrcMsgIdForMessageType(messageType);
    return NMI_PREFIX + "/" + geohash + "/" + dsrcMsgId;
  }

  /**
   * Builds NMI topic set for TIM regions.
   */
  public static Set<String> getTimTopicList(TravelerDataFrameList dataFramesList, int precision,
      EtxMessageType messageType) {
    Set<String> topics = new HashSet<>();
    for (TravelerDataFrame dataFrame : dataFramesList) {
      for (GeographicalPath region : dataFrame.getRegions()) {
        Position3D refPoint = region.getAnchor();
        if (refPoint == null) {
          continue;
        }
        double latitude = PositionConversionUtil.convertRefPointToLat(refPoint);
        double longitude = PositionConversionUtil.convertRefPointToLon(refPoint);
        topics.add(buildTopicFromCoordinates(latitude, longitude, precision, messageType));
      }
    }
    return topics;
  }

  /**
   * Builds NMI topic set for SPAT intersections.
   */
  public static Set<String> getSpatTopicList(SPAT spatMsg, MapRefPointCollector mapDataCollector,
      int precision, EtxMessageType messageType) {
    Set<String> topics = new HashSet<>();
    for (IntersectionState intersection : spatMsg.getIntersections()) {
      Position3D refPoint =
          mapDataCollector.getIntersectionRefPoint(intersection.getId().getId().toString());
      if (refPoint == null) {
        continue;
      }
      double latitude = PositionConversionUtil.convertRefPointToLat(refPoint);
      double longitude = PositionConversionUtil.convertRefPointToLon(refPoint);
      topics.add(buildTopicFromCoordinates(latitude, longitude, precision, messageType));
    }
    return topics;
  }

  /**
   * Builds NMI topic set for MAP intersections.
   */
  public static Set<String> getMapTopicList(MapData mapMsg, int precision,
      EtxMessageType messageType) {
    Set<String> topics = new HashSet<>();
    if (mapMsg == null || mapMsg.getIntersections() == null) {
      return topics;
    }
    for (IntersectionGeometry intersection : mapMsg.getIntersections()) {
      Position3D refPoint = intersection.getRefPoint();
      if (refPoint == null) {
        continue;
      }
      double latitude = PositionConversionUtil.convertRefPointToLat(refPoint);
      double longitude = PositionConversionUtil.convertRefPointToLon(refPoint);
      topics.add(buildTopicFromCoordinates(latitude, longitude, precision, messageType));
    }
    return topics;
  }
}
