package us.dot.its.jpo.ode.mec.deposit.utils;

import static org.junit.jupiter.api.Assertions.*;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import us.dot.its.jpo.ode.plugin.j2735.*;

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
    OdePosition3D result = collector.getIntersectionRefPoint(SAMPLE_INTERSECTION_ID);

    assertNotNull(result);
    assertEquals(BigDecimal.valueOf(38.9549984), result.getLatitude());
    assertEquals(BigDecimal.valueOf(-77.1493367), result.getLongitude());
  }

  @Test
  void testGetIntersectionRefPoint_NoIntersection() {
    collector.jsonMapListener(sampleMapJson);
    OdePosition3D result = collector.getIntersectionRefPoint("9999");

    assertNull(result);
  }
}
