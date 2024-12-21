package us.dot.its.jpo.ode.mec.deposit.imp.depositors.mqtt;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpProperties;
import us.dot.its.jpo.ode.mec.deposit.imp.mqtt.ImpMqttService;
import us.dot.its.jpo.ode.mec.deposit.imp.mqtt.ImpMqttTopicBuilder;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttMessageType;
import us.dot.its.jpo.ode.mec.deposit.utils.MapRefPointCollector;
import us.dot.its.jpo.ode.model.OdeSpatData;
import us.dot.its.jpo.ode.plugin.j2735.J2735IntersectionState;
import us.dot.its.jpo.ode.plugin.j2735.J2735SPAT;
import us.dot.its.jpo.ode.plugin.j2735.OdePosition3D;

/**
 * Depositor class for handling SPAT messages via MQTT integration with IMP.
 */
@Component
@Slf4j
public class ImpSpatMqttDepositor extends AbstractImpMqttDepositor {

  @Autowired
  private MapRefPointCollector mapDataCollector;

  public ImpSpatMqttDepositor(ImpProperties impProperties, ImpMqttService mqttService,
      MeterRegistry registry) {
    super(impProperties, mqttService, ImpMqttMessageType.SPAT, registry);
  }

  /**
   * Listens for SPAT messages from Kafka and deposits them to appropriate MQTT topics.
   *
   * @param message The SPAT message from Kafka in JSON format
   */
  @ConditionalOnProperty(value = {"depositor.spat.enabled", "depositor.imp.enabled"},
      havingValue = "true")
  @Async("kafkaListenerExecutor")
  @KafkaListener(topics = "${depositor.spat.source-mqtt-kafka-topic}",
      groupId = "${spring.kafka.consumer.group-id}-spat-mqtt-depositor",
      concurrency = "${listen.concurrency:1}", containerFactory = "kafkaListenerContainerFactory")
  public void spatDepositListener(String message) {
    boolean retain = false;
    try {
      final LocalDateTime startTime = LocalDateTime.now(ZoneOffset.UTC);
      OdeSpatData msg = mapper.readValue(message, OdeSpatData.class);

      String odeReceivedAt = msg.getMetadata().getOdeReceivedAt();
      if (isMessageStale(odeReceivedAt)) {
        return;
      }

      String asn1String = msg.getMetadata().getAsn1();

      J2735SPAT spatMsg = (J2735SPAT) msg.getPayload().getData();
      List<J2735IntersectionState> intersections =
          spatMsg.getIntersectionStateList().getIntersectionStatelist();

      List<String> topicList = new ArrayList<>();

      for (J2735IntersectionState intersection : intersections) {
        String intersectionId = intersection.getId().getId().toString();
        OdePosition3D refPoint = mapDataCollector.getIntersectionRefPoint(intersectionId);
        if (refPoint == null) {
          log.warn("No refPoint found for intersectionId: {} skipping IMP deposit", intersectionId);
          continue;
        }

        String topic =
            ImpMqttTopicBuilder.buildRegionalTopic(messageType, refPoint, 7, impProperties);

        topicList.add(topic);
      }

      byte[] asn1Bytes = Hex.decode(asn1String);

      for (String topic : topicList) {
        mqttService.publishAsn1Bytes(topic, asn1Bytes, retain);
        log.debug("Sending SPAT message to MQTT topics: {}", topic);
      }

      recordLatency(odeReceivedAt, startTime);

    } catch (Exception e) {
      log.error("Error processing SPaT message", e);
    }
  }
}
