package us.dot.its.jpo.ode.mec.deposit.utils;

import us.dot.its.jpo.ode.plugin.j2735.common.Position3D;

/**
 * Utility class for converting between different position formats
 */
public class PositionConversionUtil {
  private static final double DEFAULT_POSITION_SCALE = 10000000.0;

  /**
   * Converts a Position3D reference point to latitude
   * 
   * @param refPoint The reference point to convert
   * @return The latitude value
   */
  public static double convertRefPointToLat(Position3D refPoint) {
    return refPoint.getLat().getValue() / DEFAULT_POSITION_SCALE;
  }

  /**
   * Converts a Position3D reference point to longitude
   * 
   * @param refPoint The reference point to convert
   * @return The longitude value
   */
  public static double convertRefPointToLon(Position3D refPoint) {
    return refPoint.getLong_().getValue() / DEFAULT_POSITION_SCALE;
  }
}
