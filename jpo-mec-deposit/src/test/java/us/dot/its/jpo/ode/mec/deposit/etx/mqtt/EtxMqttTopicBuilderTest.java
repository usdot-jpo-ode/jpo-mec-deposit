package us.dot.its.jpo.ode.mec.deposit.etx.mqtt;

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
import us.dot.its.jpo.ode.mec.deposit.etx.models.EtxClientSubType;
import us.dot.its.jpo.ode.mec.deposit.etx.models.EtxClientType;
import us.dot.its.jpo.ode.mec.deposit.etx.models.mqtt.EtxMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.etx.models.mqtt.EtxMqttMessageType;
import us.dot.its.jpo.ode.mec.deposit.utils.MapRefPointCollector;
import us.dot.its.jpo.ode.plugin.j2735.J2735SPAT;
import us.dot.its.jpo.ode.plugin.j2735.OdePosition3D;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.TravelerDataFrameList;

/**
 * Unit tests for the EtxMqttTopicBuilder class. Tests topic construction and formatting for various
 * message types.
 */
public class EtxMqttTopicBuilderTest {

  private EtxProperties etxProperties;
  private EtxMqttProperties mqttProperties;
  private ObjectMapper objectMapper;

  @BeforeEach
  void setUp() {
    etxProperties = mock(EtxProperties.class);
    mqttProperties = mock(EtxMqttProperties.class);
    objectMapper = new ObjectMapper();

    when(etxProperties.getMqtt()).thenReturn(mqttProperties);
    when(etxProperties.getClientType()).thenReturn(EtxClientType.SOFTWARE);
    when(etxProperties.getClientSubType()).thenReturn(EtxClientSubType.APPLICATION);
    when(mqttProperties.getVendor()).thenReturn("TEST_VENDOR");
    when(mqttProperties.getMessageFormat()).thenReturn(EtxMqttMessageFormat.J2735_GR);
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
  void testBuildRegionalTopic() {
    OdePosition3D refPoint = new OdePosition3D();
    refPoint.setLatitude(new BigDecimal(42.0));
    refPoint.setLongitude(new BigDecimal(-83.0));

    String topic =
        EtxMqttTopicBuilder.buildRegionalTopic(EtxMqttMessageType.SPAT, refPoint, 7, etxProperties);

    assertTrue(topic.startsWith("vzimp/1"));
    assertTrue(topic.contains("Software"));
    assertTrue(topic.contains("TEST_VENDOR"));
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

    Set<String> topics = EtxMqttTopicBuilder.getSpatTopicList(spatMsg, etxProperties, collector);

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

    Set<String> topics = EtxMqttTopicBuilder.getTimTopicList(dataFramesList, etxProperties);

    assertFalse(topics.isEmpty());
    assertEquals(1, topics.size()); // Sample message has 4 regions
  }
}
