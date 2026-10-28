package us.dot.its.jpo.ode.mec.deposit.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.hsr.geohash.GeoHash;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import us.dot.its.jpo.ode.mec.deposit.models.exceptions.NoAvailableGeohashException;
import us.dot.its.jpo.ode.rtk_asn1_bridge.models.PolygonMessage;

class GeohashUtilsTest {

  private final GeometryFactory geometryFactory = new GeometryFactory();
  private GeohashUtils geohashUtils;

  @BeforeEach
  void setUp() {
    geohashUtils = new GeohashUtils();
  }

  @Test
  void convertPolygonMessageToPolygon_requiresAtLeastThreeVertices() {
    PolygonMessage message = PolygonMessage.newBuilder()
        .setPolygon(us.dot.its.jpo.ode.rtk_asn1_bridge.models.Polygon.newBuilder()
            .addVertices(us.dot.its.jpo.ode.rtk_asn1_bridge.models.Coordinate.newBuilder()
                .setLatitude(40.7608).setLongitude(-111.8910).build())
            .addVertices(us.dot.its.jpo.ode.rtk_asn1_bridge.models.Coordinate.newBuilder()
                .setLatitude(40.7610).setLongitude(-111.8900).build())
            .build())
        .build();

    assertThrows(IllegalArgumentException.class,
        () -> geohashUtils.convertPolygonMessageToPolygon(message));
  }

  @Test
  void convertPolygonMessageToPolygon_closesRingAndPreservesCoordinateOrder() {
    PolygonMessage message = PolygonMessage.newBuilder()
        .setPolygon(us.dot.its.jpo.ode.rtk_asn1_bridge.models.Polygon.newBuilder()
            .addVertices(us.dot.its.jpo.ode.rtk_asn1_bridge.models.Coordinate.newBuilder()
                .setLatitude(40.7608).setLongitude(-111.8910).build())
            .addVertices(us.dot.its.jpo.ode.rtk_asn1_bridge.models.Coordinate.newBuilder()
                .setLatitude(40.7610).setLongitude(-111.8900).build())
            .addVertices(us.dot.its.jpo.ode.rtk_asn1_bridge.models.Coordinate.newBuilder()
                .setLatitude(40.7600).setLongitude(-111.8895).build())
            .build())
        .build();

    Polygon polygon = geohashUtils.convertPolygonMessageToPolygon(message);
    Coordinate[] coords = polygon.getCoordinates();

    assertEquals(4, coords.length);
    assertEquals(-111.8910, coords[0].x, 0.0000001);
    assertEquals(40.7608, coords[0].y, 0.0000001);
    assertEquals(coords[0].x, coords[coords.length - 1].x, 0.0);
    assertEquals(coords[0].y, coords[coords.length - 1].y, 0.0);
  }

  @Test
  void calculateLatStep_matchesExpectedValue() {
    int precision = 7;
    int bits = precision * 5;
    int latBits = bits / 2;
    double expected = 90.0 / Math.pow(2, latBits);

    double actual = geohashUtils.calculateLatStep(precision);
    double secondCall = geohashUtils.calculateLatStep(precision);

    assertEquals(expected, actual, 0.0);
    assertEquals(actual, secondCall, 0.0);
  }

  @Test
  void calculateLonStep_matchesExpectedValue() {
    int precision = 7;
    int bits = precision * 5;
    int lonBits = (bits + 1) / 2;
    double expected = 180.0 / Math.pow(2, lonBits);

    double actual = geohashUtils.calculateLonStep(precision);
    double secondCall = geohashUtils.calculateLonStep(precision);

    assertEquals(expected, actual, 0.0);
    assertEquals(actual, secondCall, 0.0);
  }

  @Test
  void processPolygon_populatesAffectedAndRepresentativeGeohashes() {
    Polygon polygon = buildSimplePolygon();
    int precision = 6;
    int gridSize = 3;
    double latStep = geohashUtils.calculateLatStep(precision);
    double lonStep = geohashUtils.calculateLonStep(precision);
    Set<String> affectedGeohashes = new HashSet<>();
    List<String> representativeGeohashes = new ArrayList<>();

    geohashUtils.processPolygon(polygon, precision, gridSize, latStep, lonStep, affectedGeohashes,
        representativeGeohashes, Set.of());

    assertFalse(affectedGeohashes.isEmpty());
    assertFalse(representativeGeohashes.isEmpty());
    assertTrue(affectedGeohashes.containsAll(representativeGeohashes));
  }

  @Test
  void processPolygon_throwsWhenAll3x3CandidatesAlreadyUsed() {
    Polygon polygon = buildSimplePolygon();
    int precision = 6;
    int gridSize = 3;
    double latStep = geohashUtils.calculateLatStep(precision);
    double lonStep = geohashUtils.calculateLonStep(precision);
    Set<String> affectedGeohashes = new HashSet<>();
    List<String> representativeGeohashes = new ArrayList<>();

    Coordinate firstExterior = polygon.getExteriorRing().getCoordinates()[0];
    Set<String> existingUsedGeohashes =
        generate3x3Grid(firstExterior.y, firstExterior.x, precision, gridSize, latStep, lonStep);

    assertThrows(NoAvailableGeohashException.class,
        () -> geohashUtils.processPolygon(polygon, precision, gridSize, latStep, lonStep,
            affectedGeohashes, representativeGeohashes, existingUsedGeohashes));
  }

  private Polygon buildSimplePolygon() {
    Coordinate[] coords = new Coordinate[] {new Coordinate(-111.8910, 40.7608),
        new Coordinate(-111.8890, 40.7608), new Coordinate(-111.8890, 40.7620),
        new Coordinate(-111.8910, 40.7620), new Coordinate(-111.8910, 40.7608)};
    return geometryFactory.createPolygon(coords);
  }

  private Set<String> generate3x3Grid(double latitude, double longitude, int precision,
      int gridSize, double latStep, double lonStep) {
    Set<String> geohashes = new HashSet<>();
    int halfGrid = gridSize / 2;
    double startLat = latitude - (halfGrid * latStep);
    double startLon = longitude - (halfGrid * lonStep);

    for (int i = 0; i < gridSize; i++) {
      double currentLat = startLat + (i * latStep);
      currentLat = Math.max(-90.0, Math.min(90.0, currentLat));
      for (int j = 0; j < gridSize; j++) {
        double currentLon = startLon + (j * lonStep);
        currentLon = Math.max(-180.0, Math.min(180.0, currentLon));
        geohashes.add(GeoHash.withCharacterPrecision(currentLat, currentLon, precision).toBase32());
      }
    }

    return geohashes;
  }
}
