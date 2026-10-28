package us.dot.its.jpo.ode.mec.deposit.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.models.exceptions.NoAvailableGeohashException;
import us.dot.its.jpo.ode.rtk_asn1_bridge.models.PolygonMessage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import ch.hsr.geohash.GeoHash;

@Component
@Slf4j
public class GeohashUtils {


  private final Map<Integer, Double> latStepCache = new HashMap<>();
  private final Map<Integer, Double> lonStepCache = new HashMap<>();
  private final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  public Polygon convertPolygonMessageToPolygon(PolygonMessage message) {
    if (message.getPolygon().getVerticesCount() < 3) {
      throw new IllegalArgumentException(
          "PolygonMessage polygon must contain at least 3 vertices.");
    }
    List<us.dot.its.jpo.ode.rtk_asn1_bridge.models.Coordinate> vertices =
        message.getPolygon().getVerticesList();
    List<org.locationtech.jts.geom.Coordinate> coordinates = new ArrayList<>();
    for (int i = 0; i < vertices.size(); i++) {
      us.dot.its.jpo.ode.rtk_asn1_bridge.models.Coordinate vertex = vertices.get(i);
      coordinates.add(
          new org.locationtech.jts.geom.Coordinate(vertex.getLongitude(), vertex.getLatitude()));
    }
    org.locationtech.jts.geom.Coordinate first = coordinates.get(0);
    org.locationtech.jts.geom.Coordinate last = coordinates.get(coordinates.size() - 1);
    if (!first.equals2D(last)) {
      coordinates.add(first);
    }
    return GEOMETRY_FACTORY
        .createPolygon(coordinates.toArray(org.locationtech.jts.geom.Coordinate[]::new));
  }

  /**
   * Calculate latitude step size for geohash grid
   */
  public double calculateLatStep(int precision) {
    return latStepCache.computeIfAbsent(precision, p -> {
      // More accurate step size based on geohash precision
      // Geohash uses base32 encoding, so each character represents 5 bits
      // For latitude: 90 degrees / 2^(bits/2)
      int bits = p * 5;
      int latBits = bits / 2;
      return 90.0 / Math.pow(2, latBits);
    });
  }

  /**
   * Calculate longitude step size for geohash grid
   */
  public double calculateLonStep(int precision) {
    return lonStepCache.computeIfAbsent(precision, p -> {
      // More accurate step size based on geohash precision
      // For longitude: 180 degrees / 2^(bits/2)
      int bits = p * 5;
      int lonBits = (bits + 1) / 2; // Longitude gets the extra bit for odd precision
      return 180.0 / Math.pow(2, lonBits);
    });
  }

  /**
   * Process a polygon with filtering
   */
  public void processPolygon(Polygon polygon, int gridPrecision, int gridSize, double latStep,
      double lonStep, Set<String> affectedGeohashes, List<String> representativeGeohashes,
      Set<String> existingUsedGeohashes) {
    // Process exterior ring
    Coordinate[] exteriorCoords = polygon.getExteriorRing().getCoordinates();
    for (Coordinate coord : exteriorCoords) {
      processCoordinate(coord.y, coord.x, gridPrecision, gridSize, latStep, lonStep,
          affectedGeohashes, representativeGeohashes, existingUsedGeohashes);
    }

    // Process interior rings (holes)
    for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
      Coordinate[] interiorCoords = polygon.getInteriorRingN(i).getCoordinates();
      for (Coordinate coord : interiorCoords) {
        processCoordinate(coord.y, coord.x, gridPrecision, gridSize, latStep, lonStep,
            affectedGeohashes, representativeGeohashes, existingUsedGeohashes);
      }
    }

    // Process interpolated points for better coverage
    processPolygonInterpolation(polygon, gridPrecision, gridSize, latStep, lonStep,
        affectedGeohashes, representativeGeohashes, existingUsedGeohashes);
  }

  /**
   * Generate a 3x3 grid with pre-calculated step sizes for better performance
   */
  private List<String> generate3x3GridOptimized(double latitude, double longitude, int precision,
      int gridSize, double latStep, double lonStep) {
    List<String> geohashes = new ArrayList<>(gridSize * gridSize);

    // Calculate the offset to center the grid around the given point
    int halfGrid = gridSize / 2;
    double startLat = latitude - (halfGrid * latStep);
    double startLon = longitude - (halfGrid * lonStep);

    // Generate the grid
    for (int i = 0; i < gridSize; i++) {
      double currentLat = startLat + (i * latStep);
      currentLat = Math.max(-90.0, Math.min(90.0, currentLat));

      for (int j = 0; j < gridSize; j++) {
        double currentLon = startLon + (j * lonStep);
        currentLon = Math.max(-180.0, Math.min(180.0, currentLon));

        geohashes.add(encodeGeohash(currentLat, currentLon, precision));
      }
    }

    return geohashes;
  }

  /**
   * Process polygon interpolation
   */
  private void processPolygonInterpolation(Polygon polygon, int gridPrecision, int gridSize,
      double latStep, double lonStep, Set<String> affectedGeohashes,
      List<String> representativeGeohashes, Set<String> existingUsedGeohashes) {
    try {
      // Get the bounding box
      Envelope envelope = polygon.getEnvelopeInternal();

      // Generate geohashes in a grid pattern and filter by polygon intersection
      for (double lat = envelope.getMinY(); lat <= envelope.getMaxY(); lat += latStep) {
        for (double lon = envelope.getMinX(); lon <= envelope.getMaxX(); lon += lonStep) {
          // Create a point for intersection testing
          org.locationtech.jts.geom.Point point =
              GEOMETRY_FACTORY.createPoint(new Coordinate(lon, lat));

          // Check if the point intersects with the polygon
          if (polygon.contains(point) || polygon.intersects(point)) {
            processCoordinate(lat, lon, gridPrecision, gridSize, latStep, lonStep,
                affectedGeohashes, representativeGeohashes, existingUsedGeohashes);
          }
        }
      }
    } catch (NoAvailableGeohashException e) {
      log.warn("Error in polygon interpolation with filtering: {}", e.getMessage());
      throw e;
    } catch (Exception e) {
      log.warn("Error in polygon interpolation with filtering: {}", e.getMessage());
      throw new NoAvailableGeohashException(e.getMessage(), e);
    }
  }


  /**
   * Process a single coordinate with filtering - this is the core filtering logic
   */
  private void processCoordinate(double lat, double lon, int gridPrecision, int gridSize,
      double latStep, double lonStep, Set<String> affectedGeohashes,
      List<String> representativeGeohashes, Set<String> existingUsedGeohashes) {
    try {
      // Generate the geohash for this coordinate
      String geohash = encodeGeohash(lat, lon, gridPrecision);

      // Skip if this geohash is already covered by a previous 3x3 grid
      if (affectedGeohashes.contains(geohash)) {
        return;
      }

      // Generate the 3x3 grid around this coordinate
      List<String> gridGeohashes =
          generate3x3GridOptimized(lat, lon, gridPrecision, gridSize, latStep, lonStep);

      // Check if any of the geohashes in this grid are already covered
      boolean hasOverlap = false;
      for (String gridGeohash : gridGeohashes) {
        if (affectedGeohashes.contains(gridGeohash)) {
          hasOverlap = true;
          break;
        }
      }

      // Only add this representative if there's no overlap with grids in this run
      if (!hasOverlap) {
        // Choose a representative geohash, preferring center but avoiding already-used
        // ones
        String chosen = null;
        // Center first
        if ((existingUsedGeohashes == null || !existingUsedGeohashes.contains(geohash))
            && !representativeGeohashes.contains(geohash)) {
          chosen = geohash;
        } else {
          // Try neighbors not in existing used set
          for (String candidate : gridGeohashes) {
            if (candidate.equals(geohash)) {
              continue;
            }
            if ((existingUsedGeohashes == null || !existingUsedGeohashes.contains(candidate))
                && !representativeGeohashes.contains(candidate)) {
              chosen = candidate;
              break;
            }
          }
        }
        // If all 3x3 candidates are already used, fail immediately
        if (chosen == null) {
          throw new NoAvailableGeohashException(
              "No available representative geohash for 3x3 grid at lat=" + lat + ", lon=" + lon);
        }
        // Mark all geohashes in this grid as affected and record representative
        affectedGeohashes.addAll(gridGeohashes);
        representativeGeohashes.add(chosen);
      }
    } catch (NoAvailableGeohashException e) {
      log.warn("No available representative geohash: {}", e.getMessage());
      throw e;
    } catch (Exception e) {
      log.warn("Error processing coordinate ({}, {}) with filtering: {}", lat, lon, e.getMessage());
      throw new NoAvailableGeohashException(e.getMessage(), e);
    }
  }

  /**
   * Encode latitude and longitude to geohash using ch.hsr.geohash library
   *
   * @param latitude Latitude in degrees
   * @param longitude Longitude in degrees
   * @param precision Geohash precision (number of characters)
   * @return Geohash string
   * @throws RuntimeException if geohash encoding fails
   */
  private String encodeGeohash(double latitude, double longitude, int precision) {
    try {
      // Use ch.hsr.geohash library for accurate geohash encoding
      GeoHash geoHash = GeoHash.withCharacterPrecision(latitude, longitude, precision);
      return geoHash.toBase32();
    } catch (Exception e) {
      String errorMessage =
          String.format("Error encoding geohash for lat=%.6f, lon=%.6f, precision=%d:%s", latitude,
              longitude, precision, e.getMessage());
      log.error(errorMessage, e);
      throw new RuntimeException(errorMessage, e);
    }
  }
}
