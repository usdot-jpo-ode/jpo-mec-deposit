package us.dot.its.jpo.ode.mec.deposit.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.model.OdeMapData;
import us.dot.its.jpo.ode.plugin.j2735.J2735IntersectionGeometry;
import us.dot.its.jpo.ode.plugin.j2735.J2735IntersectionGeometryList;
import us.dot.its.jpo.ode.plugin.j2735.J2735MAP;
import us.dot.its.jpo.ode.plugin.j2735.OdePosition3D;

/**
 * Collects and manages reference points from MAP messages for intersections.
 */
@Component
@Slf4j
public class MapRefPointCollector {
  private final ObjectMapper mapper = DateJsonMapper.getInstance();
  private ConcurrentHashMap<String, OdePosition3D> map = new ConcurrentHashMap<>();

  /**
   * Listens for and processes MAP messages from Kafka.
   *
   * @param message The JSON MAP message
   */
  @KafkaListener(topics = "${imp.depositors.map.mqtt.kafka-topic}",
      groupId = "${spring.kafka.consumer.group-id}-map-collector",
      concurrency = "${listen.concurrency:1}", properties = {"auto.offset.reset=earliest"})
  public void jsonMapListener(String message) {
    try {
      OdeMapData msg = mapper.readValue(message, OdeMapData.class);
      J2735MAP mapMsg = (J2735MAP) msg.getPayload().getData();
      J2735IntersectionGeometryList intersections = mapMsg.getIntersections();

      for (int i = 0; i < intersections.getIntersections().size(); i++) {
        J2735IntersectionGeometry intersection = intersections.getIntersections().get(i);
        String intersectionId = intersection.getId().getId().toString();
        OdePosition3D refPoint = intersection.getRefPoint();
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
  public OdePosition3D getIntersectionRefPoint(String intersectionId) {
    if (intersectionId == null) {
      log.error("Intersection ID cannot be null");
      throw new IllegalArgumentException("Intersection ID cannot be null");
    }

    OdePosition3D position = map.get(intersectionId);
    if (position == null) {
      log.warn("No reference point found for intersection ID: {}", intersectionId);
      return null;
    }

    return position;
  }
}
