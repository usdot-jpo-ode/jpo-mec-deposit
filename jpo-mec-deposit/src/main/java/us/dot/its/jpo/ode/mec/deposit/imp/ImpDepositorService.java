package us.dot.its.jpo.ode.mec.deposit.imp;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

import us.dot.its.jpo.ode.mec.deposit.DateJsonMapper;
import us.dot.its.jpo.ode.mec.deposit.DepositorProperties;
import us.dot.its.jpo.ode.mec.deposit.models.ode.OdeTimData;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import us.dot.its.jpo.ode.mec.deposit.GeoRoutedMsg;
import us.dot.its.jpo.ode.model.OdeSpatData;

@Component
@Slf4j
public class ImpDepositorService {
    private final ObjectMapper mapper = DateJsonMapper.getInstance();
    private final ImpMqttService mqttService;
    private final DepositorProperties properties;

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

    @KafkaListener(topics = "topic.OdeTimJsonTMCFiltered", groupId = "jpo-s3-depositor", concurrency = "${listen.concurrency:1}")
    public void tmcTimListener(String message) {
        boolean retain = true;
        try {
            OdeTimData timMsg = mapper.readValue(message, OdeTimData.class);
            String asn1String = timMsg.getMetadata().getAsn1();
            String odeReceivedAt = timMsg.getMetadata().getOdeReceivedAt();
            // GeoRoutedMsg geoRoutedMsg = ImpUtil.getGeoRoutedMsg(asn1String,
            // odeReceivedAt, 0.0, 0.0);
            // List<String> topicList = ImpUtil.getRegionalTimTopicList(timMsg, properties);

            // log.info("Received TIM message: {}", geoRoutedMsg);
            // log.info("Sending TIM message to MQTT topics: {}", topicList);

            // for (String topic : topicList) {
            // mqttService.publish(topic, geoRoutedMsg, retain);
            // }

            List<double[]> pathCoords = ImpUtil.getTimPathCoordList(timMsg);
            List<double[]> geofence = ImpUtil.generateGeofence(pathCoords, 50.0); // 50 meter buffer

        } catch (Exception e) {
            log.error("Error processing TIM message", e);
            // Handle exception
        }
    }

    @KafkaListener(topics = "topic.OdeSpatJson", groupId = "jpo-s3-depositor", concurrency = "${listen.concurrency:1}")
    public void jsonSpatListener(String message) {
        boolean retain = false;
        try {
            double latitude = 40.47387909290231;
            double longitude = -104.96936518350597;
            OdeSpatData msg = mapper.readValue(message, OdeSpatData.class);
            String asn1String = msg.getMetadata().getAsn1();
            String odeReceivedAt = msg.getMetadata().getOdeReceivedAt();
            // Parse the odeReceivedAt into a datetime object
            LocalDateTime receivedAt = LocalDateTime.parse(odeReceivedAt, DateTimeFormatter.ISO_DATE_TIME);
            // Get the current local datetime
            LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
            // Calculate the latency
            Duration latency = Duration.between(receivedAt, now);
            log.info("topic.OdeSpatJson Latency: {} milliseconds", latency.toMillis());
            GeoRoutedMsg geoRoutedMsg = ImpUtil.getGeoRoutedMsg(asn1String, odeReceivedAt, latitude, longitude);

            List<String> topicList = ImpUtil.getGenericTopic(latitude, longitude, 7, "TIM", properties);

            log.debug("Received SPAT message: {}", geoRoutedMsg);
            log.debug("Sending SPAT message to MQTT topics: {}", topicList);

            for (String topic : topicList) {
                mqttService.publish(topic, geoRoutedMsg, retain);
            }

        } catch (Exception e) {
            log.error("Error processing TIM message", e);
            // Handle exception
        }
    }

}
