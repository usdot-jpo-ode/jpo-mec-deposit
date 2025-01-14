package us.dot.its.jpo.ode.mec.deposit.utils.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientSubType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;
import us.dot.its.jpo.ode.mec.deposit.utils.MapRefPointCollector;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttTopicBuilder;
import us.dot.its.jpo.ode.plugin.j2735.J2735SPAT;
import us.dot.its.jpo.ode.plugin.j2735.OdePosition3D;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.TravelerDataFrameList;

/**
 * Unit tests for the EtxMqttTopicBuilder class. Tests topic construction and formatting for various
 * message types.
 */
public class EtxMqttTopicBuilderTest {

  private ObjectMapper objectMapper;

  @BeforeEach
  void setUp() {
    objectMapper = new ObjectMapper();
  }

  @Test
  void testGetPubTopicGeoHash() {
    String result = EtxMqttTopicBuilder.getPubTopicGeoHash("abc123", 7);
    assertEquals("a/b/c/1/2/3/-/-", result);
  }

  @Test
  void testGetPubTopicGeoHashWithInvalidPrecision() {
    assertThrows(IllegalArgumentException.class, () -> {
      EtxMqttTopicBuilder.getPubTopicGeoHash("abc123", 5);
    });
  }

  @Test
  void testGetPubTopicGeoHashWithNullInput() {
    assertThrows(IllegalArgumentException.class, () -> {
      EtxMqttTopicBuilder.getPubTopicGeoHash(null, 7);
    });
  }

  @Test
  void testGetSpatTopicList() throws IOException {
    // Load sample SPAT message
    JsonNode spatJson = objectMapper
        .readTree(getClass().getResourceAsStream("/sample_messages/sample-ode-spat.json"));
    final J2735SPAT spatMsg =
        objectMapper.convertValue(spatJson.get("payload").get("data"), J2735SPAT.class);

    // Setup MapRefPointCollector with the reference point from MAP message
    JsonNode mapJson = objectMapper
        .readTree(getClass().getResourceAsStream("/sample_messages/sample-ode-map.json"));
    JsonNode refPointJson = mapJson.get("payload").get("data").get("intersections")
        .get("intersectionGeometry").get(0).get("refPoint");

    MapRefPointCollector collector = mock(MapRefPointCollector.class);
    OdePosition3D refPoint = new OdePosition3D();
    refPoint.setLatitude(new BigDecimal(refPointJson.get("latitude").asDouble()));
    refPoint.setLongitude(new BigDecimal(refPointJson.get("longitude").asDouble()));
    when(collector.getIntersectionRefPoint("9709")).thenReturn(refPoint);

    Set<String> topics = EtxMqttTopicBuilder.getSpatTopicList(spatMsg, collector, "TEST_VENDOR", 7,
        EtxMqttMessageFormat.J2735_GR, EtxClientType.SOFTWARE, EtxClientSubType.APPLICATION);

    assertFalse(topics.isEmpty());
    assertEquals(1, topics.size());
  }

  @Test
  void testGetTimTopicList() throws IOException {
    // Load sample TIM message
    JsonNode timJson = objectMapper
        .readTree(getClass().getResourceAsStream("/sample_messages/sample-ode-tim.json"));
    TravelerDataFrameList dataFramesList = objectMapper.convertValue(
        timJson.get("payload").get("data").get("dataFrames"), TravelerDataFrameList.class);

    Set<String> topics = EtxMqttTopicBuilder.getTimTopicList(dataFramesList, "TEST_VENDOR", 7,
        EtxMqttMessageFormat.J2735_GR, EtxClientType.SOFTWARE, EtxClientSubType.APPLICATION);

    assertFalse(topics.isEmpty());
    assertEquals(1, topics.size());
  }
}
