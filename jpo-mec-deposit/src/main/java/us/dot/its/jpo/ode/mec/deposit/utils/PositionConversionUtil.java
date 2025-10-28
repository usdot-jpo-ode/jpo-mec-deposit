package us.dot.its.jpo.ode.mec.deposit.utils;

import us.dot.its.jpo.asn.j2735.r2024.Common.Latitude;
import us.dot.its.jpo.asn.j2735.r2024.Common.Longitude;
import us.dot.its.jpo.asn.j2735.r2024.Common.Position3D;

/**
 * Utility class for converting between different position formats.
 */
public class PositionConversionUtil {
  private static final double DEFAULT_POSITION_SCALE = 10000000.0;

  /**
   * Converts a Position3D reference point to latitude.
   *
   * @param refPoint The reference point to convert
   * @return The latitude value
   */
  public static double convertRefPointToLat(Position3D refPoint) {
    return refPoint.getLat().getValue() / DEFAULT_POSITION_SCALE;
  }

  /**
   * Converts a Position3D reference point to longitude.
   *
   * @param refPoint The reference point to convert
   * @return The longitude value
   */
  public static double convertRefPointToLon(Position3D refPoint) {
    return refPoint.getLong_().getValue() / DEFAULT_POSITION_SCALE;
  }

  /**
   * Converts a J2735 latitude value to decimal degrees.
   *
   * @param latitude The latitude value to convert
   * @return The latitude value in decimal degrees
   */
  public static double convertJ2735LatToLat(Latitude latitude) {
    return latitude.getValue() / DEFAULT_POSITION_SCALE;
  }

  /**
   * Converts a J2735 longitude value to decimal degrees.
   *
   * @param longitude The longitude value to convert
   * @return The longitude value in decimal degrees
   */
  public static double convertJ2735LonToLon(Longitude longitude) {
    return longitude.getValue() / DEFAULT_POSITION_SCALE;
  }
}
