package us.dot.its.jpo.ode.mec.deposit.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import us.dot.its.jpo.asn.j2735.r2024.Common.Position3D;

class MapRefPointCollectorTest {

  private MapRefPointCollector collector;

  private static final String SAMPLE_INTERSECTION_ID = "9709";
  private String sampleMapJson;

  @BeforeEach
  void setUp() throws Exception {
    // Load sample MAP JSON from resources
    sampleMapJson = new String(Files.readAllBytes(Paths.get(
        getClass().getClassLoader().getResource("sample_messages/sample-ode-map.json").toURI())));
    collector = new MapRefPointCollector();
  }

  @Test
  void testGetIntersectionRefPoint() {
    collector.jsonMapListener(sampleMapJson);
    Position3D result = collector.getIntersectionRefPoint(SAMPLE_INTERSECTION_ID);

    assertNotNull(result);
    // Convert from microdegrees to decimal degrees (divide by 10,000,000)
    double expectedLat = 38.9549984;
    double expectedLon = -77.1493367;
    double actualLat = result.getLat().getValue() / 10000000.0;
    double actualLon = result.getLong_().getValue() / 10000000.0;

    assertEquals(expectedLat, actualLat, 0.0000001);
    assertEquals(expectedLon, actualLon, 0.0000001);
  }

  @Test
  void testGetIntersectionRefPoint_NoIntersection() {
    collector.jsonMapListener(sampleMapJson);
    Position3D result = collector.getIntersectionRefPoint("9999");

    assertNull(result);
  }
}
