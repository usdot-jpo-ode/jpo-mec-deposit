package us.dot.its.jpo.ode.mec.deposit.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.asn.j2735.r2024.Common.Position3D;
import us.dot.its.jpo.asn.j2735.r2024.MapData.IntersectionGeometry;
import us.dot.its.jpo.asn.j2735.r2024.MapData.IntersectionGeometryList;
import us.dot.its.jpo.asn.j2735.r2024.MapData.MapData;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;

/**
 * Collects and manages reference points from MAP messages for intersections.
 */
@Component
@Slf4j
public class MapRefPointCollector {
  private final ObjectMapper mapper = DateJsonMapper.getInstance();
  private ConcurrentHashMap<String, Position3D> map = new ConcurrentHashMap<>();

  /**
   * Listens for and processes MAP messages from Kafka.
   *
   * @param message The JSON MAP message
   */
  @KafkaListener(topics = "${mec-deposit.etx.mqtt-brokers.etx.depositors.map.mqtt.kafka-topic}",
      groupId = "${spring.kafka.consumer.group-id}-map-collector",
      concurrency = "${spring.kafka.listener.concurrency:1}",
      properties = {"auto.offset.reset=earliest"})
  public void jsonMapListener(String message) {
    try {
      OdeMessageFrameData msg = mapper.readValue(message, OdeMessageFrameData.class);
      MapData mapMsg = (MapData) msg.getPayload().getData().getValue();
      IntersectionGeometryList intersections = mapMsg.getIntersections();

      for (int i = 0; i < intersections.size(); i++) {
        IntersectionGeometry intersection = intersections.get(i);
        String intersectionId = intersection.getId().getId().toString();
        Position3D refPoint = intersection.getRefPoint();
        log.debug("Received MAP message: {} with refPoint: {}", intersectionId, refPoint);
        map.put(intersectionId, refPoint);
      }

    } catch (Exception e) {
      log.error("Error processing MAP message", e);
      log.error("Message content: {}", message);
      log.error("Stack trace:", e);
    }
  }

  /**
   * Gets the reference point for a specific intersection.
   *
   * @param intersectionId The intersection identifier
   * @return The intersection's reference point position, or null if not found
   * @throws IllegalArgumentException if intersectionId is null
   */
  public Position3D getIntersectionRefPoint(String intersectionId) {
    if (intersectionId == null) {
      log.error("Intersection ID cannot be null");
      throw new IllegalArgumentException("Intersection ID cannot be null");
    }

    Position3D position = map.get(intersectionId);
    if (position == null) {
      log.warn("No reference point found for intersection ID: {}", intersectionId);
      return null;
    }

    return position;
  }
}
