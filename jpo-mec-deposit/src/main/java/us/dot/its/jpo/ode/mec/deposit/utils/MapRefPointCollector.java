package us.dot.its.jpo.ode.mec.deposit.utils;

import java.util.concurrent.ConcurrentHashMap;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.ode.model.OdeMapData;
import us.dot.its.jpo.ode.plugin.j2735.J2735IntersectionGeometry;
import us.dot.its.jpo.ode.plugin.j2735.J2735IntersectionGeometryList;
import us.dot.its.jpo.ode.plugin.j2735.J2735MAP;
import us.dot.its.jpo.ode.plugin.j2735.OdePosition3D;

@Component
@Slf4j
public class MapRefPointCollector {
    private final ObjectMapper mapper = DateJsonMapper.getInstance();
    private ConcurrentHashMap<String, OdePosition3D> map = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        log.debug("MapRefPointCollector PostConstruct initialization");
    }

    public MapRefPointCollector() {
        log.debug("MapRefPointCollector initialized");
    }

    @KafkaListener(topics = "topic.OdeMapJson", groupId = "${spring.kafka.consumer.group-id}-map", concurrency = "${listen.concurrency:1}", properties = {
            "auto.offset.reset=earliest" })
    public void jsonMapListener(String message) {
        log.debug("Received message on topic.OdeMapJson: {}",
                message.substring(0, Math.min(message.length(), 100)));
        try {
            OdeMapData msg = mapper.readValue(message, OdeMapData.class);
            J2735MAP mapMsg = (J2735MAP) msg.getPayload().getData();
            J2735IntersectionGeometryList intersections = mapMsg.getIntersections();

            log.debug("Processing MAP message with {} intersections",
                    intersections.getIntersections().size());

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
