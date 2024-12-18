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
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import us.dot.its.jpo.ode.mec.deposit.common.MapDataCollector;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.MessageFormat;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.MessageType;
import us.dot.its.jpo.ode.model.OdeBsmData;
import us.dot.its.jpo.ode.model.OdeSpatData;
import us.dot.its.jpo.ode.plugin.j2735.J2735Bsm;
import us.dot.its.jpo.ode.plugin.j2735.J2735IntersectionState;
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

    public ImpDepositorService(DepositorProperties properties, ImpMqttService mqttService) {
        this.properties = properties;
        this.mqttService = mqttService;

        var registration = new ImpPartnerApi(properties);
        var response = registration.registerClientPartner();

        if (response != null) {
            log.info("IMP registration successful");
        } else {
            log.error("IMP registration failed, services will not be started");
        }
    }

    // @Override
    // public void run() {
    // // Logic to start the service
    // log.info("ImpDepositorService is running");
    // // You can add any initialization logic here if
    // needed
    // }

    // @KafkaListener(topics = "topic.OdeTimJson",
    // groupId =
    // "${spring.kafka.consumer.group-id}-tim",
    // concurrency =
    // "${listen.concurrency:1}")
    // public void tmcTimListener(String message) {
    // boolean retain = true;
    // try {
    // OdeTimData timMsg = mapper.readValue(message,
    // OdeTimData.class);
    // String asn1String =
    // timMsg.getMetadata().getAsn1();
    // String odeReceivedAt =
    // timMsg.getMetadata().getOdeReceivedAt();

    // List<double[]> geofence =
    // ImpUtil.generateGeofence(pathCoords, 50.0); //
    // 50

    // } catch (Exception e) {
    // log.error("Error processing TIM message", e);
    // // Handle exception
    // }
    // }

    @Async("kafkaListenerExecutor")
    @KafkaListener(topics = "topic.OdeSpatJson", groupId = "${spring.kafka.consumer.group-id}-spat", concurrency = "${listen.concurrency:1}", containerFactory = "fastKafkaListenerContainerFactory")
    public void spatDepositListener(String message) {
        boolean retain = false;
        try {
            LocalDateTime startTime = LocalDateTime.now(ZoneOffset.UTC);

            OdeSpatData msg = mapper.readValue(message, OdeSpatData.class);
            String asn1String = msg.getMetadata().getAsn1();

            J2735SPAT spatMsg = (J2735SPAT) msg.getPayload().getData();
            List<J2735IntersectionState> intersections = spatMsg.getIntersectionStateList()
                    .getIntersectionStatelist();

            List<String> topicList = new ArrayList<>();

            for (J2735IntersectionState intersection : intersections) {
                String intersectionId = intersection.getId().getId().toString();
                OdePosition3D refPoint = mapDataCollector.getIntersectionRefPoint(intersectionId);
                if (refPoint == null) {
                    log.warn("No refPoint found for intersectionId: {} skipping IMP deposit",
                            intersectionId);
                    continue;
                }

                String topic = ImpMqttTopicBuilder.buildRegionalTopic(refPoint, 7, properties,
                        MessageFormat.J2735, MessageType.SPAT);

                topicList.add(topic);
            }

            // convert a string of hex asn1 to a byte array
            byte[] asn1Bytes = Hex.decode(asn1String);

            for (String topic : topicList) {
                mqttService.publishAsn1Bytes(topic, asn1Bytes, retain);
                log.debug("Sending SPAT message to MQTT topics: {}", topic);
            }

            LocalDateTime receivedAt = LocalDateTime.parse(msg.getMetadata().getOdeReceivedAt(),
                    DateTimeFormatter.ISO_DATE_TIME);
            Duration latency = Duration.between(receivedAt, startTime);

            log.debug("Kafka processing latency: {} milliseconds", latency.toMillis());

            if (latency.toMillis() > 25) {
                log.warn("High SPaT Kafka processing latency of: {} milliseconds",
                        latency.toMillis());
            }

        } catch (Exception e) {
            log.error("Error processing SPaT message", e);
            // Handle exception
        }
    }

    @Async("kafkaListenerExecutor")
    @KafkaListener(topics = "topic.OdeBsmJson", groupId = "${spring.kafka.consumer.group-id}-bsm", concurrency = "${listen.concurrency:1}", containerFactory = "fastKafkaListenerContainerFactory")
    public void bsmDepositListener(String message) {
        try {
            LocalDateTime startTime = LocalDateTime.now(ZoneOffset.UTC);
            OdeBsmData msg = mapper.readValue(message, OdeBsmData.class);
            byte[] asn1Bytes = Hex.decode(msg.getMetadata().getAsn1());

            J2735Bsm bsm = (J2735Bsm) msg.getPayload().getData();
            String topic = ImpMqttTopicBuilder.buildRegionalTopic(bsm.getCoreData().getPosition(),
                    7, properties, MessageFormat.J2735, MessageType.BSM);

            // Fire and forget without waiting for completion
            mqttService.publishAsn1Bytes(topic, asn1Bytes, false).exceptionally(throwable -> {
                log.error("Failed to publish BSM message: {}", throwable.getMessage());
                return null;
            });

            LocalDateTime receivedAt = LocalDateTime.parse(msg.getMetadata().getOdeReceivedAt(),
                    DateTimeFormatter.ISO_DATE_TIME);
            Duration latency = Duration.between(receivedAt, startTime);

            log.debug("Kafka processing latency: {} milliseconds", latency.toMillis());

            if (latency.toMillis() > 25) {
                log.warn("High BSM Kafka processing latency of: {} milliseconds",
                        latency.toMillis());
            }
        } catch (Exception e) {
            log.error("Error processing BSM message", e);
        }
    }

}
