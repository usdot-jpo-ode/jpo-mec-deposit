package us.dot.its.jpo.ode.mec.deposit.imp;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

import us.dot.its.jpo.ode.mec.deposit.DateJsonMapper;
import us.dot.its.jpo.ode.mec.deposit.DepositorProperties;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.bouncycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import us.dot.its.jpo.ode.mec.deposit.GeoRoutedMsg;
import us.dot.its.jpo.ode.mec.deposit.common.MapDataCollector;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.MessageFormat;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.MessageType;
import us.dot.its.jpo.ode.model.OdeBsmData;
import us.dot.its.jpo.ode.model.OdeMapData;
import us.dot.its.jpo.ode.model.OdeSpatData;
import us.dot.its.jpo.ode.model.OdeTimData;
import us.dot.its.jpo.ode.plugin.j2735.J2735Bsm;
import us.dot.its.jpo.ode.plugin.j2735.J2735IntersectionGeometry;
import us.dot.its.jpo.ode.plugin.j2735.J2735IntersectionGeometryList;
import us.dot.its.jpo.ode.plugin.j2735.J2735IntersectionState;
import us.dot.its.jpo.ode.plugin.j2735.J2735IntersectionStateList;
import us.dot.its.jpo.ode.plugin.j2735.J2735MAP;
import us.dot.its.jpo.ode.plugin.j2735.J2735SPAT;
import us.dot.its.jpo.ode.plugin.j2735.OdePosition3D;

@Component
@Slf4j
public class ImpDepositorService {
    private final ObjectMapper mapper = DateJsonMapper.getInstance();
    private final ImpMqttService mqttService;
    private final DepositorProperties properties;

    @Autowired
    private MapDataCollector mapDataCollector;

    public ImpDepositorService(DepositorProperties properties) {
        this.properties = properties;

        var registration = new ImpRegistration(properties);
        var response = registration.registerClientPartner();

        if (response != null) {
            log.info("IMP registration successful");
        } else {
            log.error("IMP registration failed, services will not be started");
        }

        this.mqttService = new ImpMqttService(properties);
    }

    // @Override
    // public void run() {
    // // Logic to start the service
    // log.info("ImpDepositorService is running");
    // // You can add any initialization logic here if needed
    // }

    // @KafkaListener(topics = "topic.OdeTimJsonTMCFiltered", groupId =
    // "${spring.kafka.consumer.group-id}-tim", concurrency =
    // "${listen.concurrency:1}")
    // public void tmcTimListener(String message) {
    // boolean retain = true;
    // try {
    // OdeTimData timMsg = mapper.readValue(message, OdeTimData.class);
    // String asn1String = timMsg.getMetadata().getAsn1();
    // String odeReceivedAt = timMsg.getMetadata().getOdeReceivedAt();
    // GeoRoutedMsg geoRoutedMsg = ImpUtil.getGeoRoutedMsg(asn1String,
    // odeReceivedAt, 0.0, 0.0);
    // List<String> topicList = ImpUtil.getRegionalTimTopicList(timMsg, properties);

    // log.info("Received TIM message: {}", geoRoutedMsg);
    // log.info("Sending TIM message to MQTT topics: {}", topicList);

    // for (String topic : topicList) {
    // mqttService.publish(topic, geoRoutedMsg, retain);
    // }

    // List<double[]> pathCoords = ImpUtil.getTimPathCoordList(timMsg);
    // List<double[]> geofence = ImpUtil.generateGeofence(pathCoords, 50.0); // 50

    // } catch (Exception e) {
    // log.error("Error processing TIM message", e);
    // // Handle exception
    // }
    // }

    @KafkaListener(topics = "topic.OdeSpatJson", groupId = "${spring.kafka.consumer.group-id}-spat", concurrency = "${listen.concurrency:1}")
    public void spatDepositListener(String message) {
        boolean retain = false;
        try {
            OdeSpatData msg = mapper.readValue(message, OdeSpatData.class);
            String asn1String = msg.getMetadata().getAsn1();
            String odeReceivedAt = msg.getMetadata().getOdeReceivedAt();

            J2735SPAT spatMsg = (J2735SPAT) msg.getPayload().getData();
            List<J2735IntersectionState> intersections = spatMsg.getIntersectionStateList().getIntersectionStatelist();

            List<String> topicList = new ArrayList<>();

            for (J2735IntersectionState intersection : intersections) {
                String intersectionId = intersection.getId().getId().toString();
                OdePosition3D refPoint = mapDataCollector.getIntersectionRefPoint(intersectionId);
                if (refPoint == null) {
                    log.debug("No refPoint found for intersectionId: {}", intersectionId);
                    continue;
                }

                // TODO: change to SPAT after VZ updates ACL permissions!!!
                String topic = ImpMqttTopicBuilder.buildRegionalTopic(refPoint, 7, properties, MessageFormat.J2735,
                        MessageType.TIM);

                topicList.add(topic);
            }

            // convert a string of hex asn1 to a byte array
            byte[] asn1Bytes = Hex.decode(asn1String);

            for (String topic : topicList) {
                mqttService.publishAsn1Bytes(topic, asn1Bytes, retain);
                log.debug("Sending SPAT message to MQTT topics: {}", topic);
            }

            LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
            LocalDateTime receivedAt = LocalDateTime.parse(odeReceivedAt, DateTimeFormatter.ISO_DATE_TIME);

            Duration latency = Duration.between(receivedAt, now);
            log.debug("topic.OdeSpatJson Latency: {} milliseconds", latency.toMillis());

        } catch (Exception e) {
            log.error("Error processing SPaT message", e);
            // Handle exception
        }
    }

    @KafkaListener(topics = "topic.OdeBsmJson", groupId = "${spring.kafka.consumer.group-id}-bsm", concurrency = "${listen.concurrency:1}")
    public void bsmDepositListener(String message) {
        boolean retain = false;
        try {
            OdeBsmData msg = mapper.readValue(message, OdeBsmData.class);
            String asn1String = msg.getMetadata().getAsn1();
            String odeReceivedAt = msg.getMetadata().getOdeReceivedAt();

            J2735Bsm bsm = (J2735Bsm) msg.getPayload().getData();
            OdePosition3D coreData = bsm.getCoreData().getPosition();

            // TODO: change to BSM after VZ updates ACL permissions!!!
            String topic = ImpMqttTopicBuilder.buildRegionalTopic(coreData, 7, properties, MessageFormat.J2735,
                    MessageType.TIM);

            // convert a string of hex asn1 to a byte array
            byte[] asn1Bytes = Hex.decode(asn1String);

            mqttService.publishAsn1Bytes(topic, asn1Bytes, retain);
            log.debug("Sending BSM message to MQTT topics: {}", topic);

            LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
            LocalDateTime receivedAt = LocalDateTime.parse(odeReceivedAt, DateTimeFormatter.ISO_DATE_TIME);

            Duration latency = Duration.between(receivedAt, now);
            log.debug("topic.OdeBsmJson Latency: {} milliseconds", latency.toMillis());

        } catch (Exception e) {
            log.error("Error processing BSM message", e);
            // Handle exception
        }
    }

}
