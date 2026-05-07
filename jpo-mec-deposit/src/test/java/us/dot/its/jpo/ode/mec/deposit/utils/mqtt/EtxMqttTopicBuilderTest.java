package us.dot.its.jpo.ode.mec.deposit.utils.mqtt;

import java.io.IOException;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientSubType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.utils.MapRefPointCollector;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;
import us.dot.its.jpo.asn.j2735.r2024.SPAT.SPAT;
import us.dot.its.jpo.asn.j2735.r2024.Common.Position3D;
import us.dot.its.jpo.asn.j2735.r2024.MapData.IntersectionGeometry;
import us.dot.its.jpo.asn.j2735.r2024.MapData.MapData;
import us.dot.its.jpo.asn.j2735.r2024.TravelerInformation.TravelerDataFrame;
import us.dot.its.jpo.asn.j2735.r2024.TravelerInformation.TravelerDataFrameList;
import us.dot.its.jpo.asn.j2735.r2024.TravelerInformation.TravelerInformation;
import us.dot.its.jpo.asn.j2735.r2024.TravelerInformation.TravelerInformationMessageFrame;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import static org.mockito.Mockito.when;

/**
 * Unit tests for the EtxMqttTopicBuilder class. Tests topic construction and formatting for various
 * message types.
 */
public class EtxMqttTopicBuilderTest {

  private ObjectMapper objectMapper;

  @BeforeEach
  void setUp() {
    objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Test
  void testGetPubTopicGeoHash() {
    String result = EtxMqttTopicBuilder.getPubTopicGeoHash("abc123", 7);
    assertEquals("a/b/c/1/2/3/-/-", result);
  }

  @Test
  void testBuildNmiTopicFromGeohash() {
    String topic = NmiMqttTopicBuilder.buildTopicFromGeohash("mqqtxyz", 7, "32");
    assertEquals("/v1/g32/m/q/q/t/x/y/z/32", topic);
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
    OdeMessageFrameData odeSpatMsg = objectMapper.convertValue(spatJson, OdeMessageFrameData.class);
    SPAT spatMsg = (SPAT) odeSpatMsg.getPayload().getData().getValue();

    // Setup MapRefPointCollector with the reference point from MAP message
    JsonNode mapJson = objectMapper
        .readTree(getClass().getResourceAsStream("/sample_messages/sample-ode-map.json"));
    OdeMessageFrameData odeMapMsg = objectMapper.convertValue(mapJson, OdeMessageFrameData.class);
    MapData mapData = (MapData) odeMapMsg.getPayload().getData().getValue();
    IntersectionGeometry intersection = mapData.getIntersections().get(0);
    Position3D refPoint = intersection.getRefPoint();
    MapRefPointCollector collector = mock(MapRefPointCollector.class);
    // Mock the collector to return null - this will test the fallback behavior
    when(collector.getIntersectionRefPoint(intersection.getId().getId().toString()))
        .thenReturn(refPoint);

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
    OdeMessageFrameData odeTimMsg = objectMapper.convertValue(timJson, OdeMessageFrameData.class);
    TravelerInformation timMsg = (TravelerInformation) odeTimMsg.getPayload().getData().getValue();
    TravelerDataFrameList dataFramesList = timMsg.getDataFrames();
    Set<String> topics = EtxMqttTopicBuilder.getTimTopicList(dataFramesList, "TEST_VENDOR", 7,
        EtxMqttMessageFormat.J2735_GR, EtxClientType.SOFTWARE, EtxClientSubType.APPLICATION);

    assertFalse(topics.isEmpty());
    assertEquals(1, topics.size());
  }
}
