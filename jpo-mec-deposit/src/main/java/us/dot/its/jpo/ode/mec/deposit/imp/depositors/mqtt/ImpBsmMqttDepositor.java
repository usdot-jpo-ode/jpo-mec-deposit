package us.dot.its.jpo.ode.mec.deposit.imp.depositors.mqtt;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpProperties;
import us.dot.its.jpo.ode.mec.deposit.imp.mqtt.ImpMqttService;
import us.dot.its.jpo.ode.mec.deposit.imp.mqtt.ImpMqttTopicBuilder;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttMessageType;
import us.dot.its.jpo.ode.model.OdeBsmData;
import us.dot.its.jpo.ode.plugin.j2735.J2735Bsm;

/**
 * Depositor class for handling BSM messages via MQTT integration with IMP.
 */
@Component
@Slf4j
@ConditionalOnProperty(value = {"depositor.bsm.mqtt.enabled", "depositor.imp.enabled"},
    havingValue = "true")
public class ImpBsmMqttDepositor extends AbstractImpMqttDepositor {

  public ImpBsmMqttDepositor(ImpProperties impProperties, ImpMqttService mqttService,
      MeterRegistry registry) {
    super(impProperties, mqttService, ImpMqttMessageType.BSM, registry);
  }

  /**
   * Listens for BSM messages from Kafka and deposits them to appropriate MQTT topics.
   *
   * @param message The BSM message from Kafka in JSON format
   */
  @Async("kafkaListenerExecutor")
  @KafkaListener(topics = "${depositor.bsm.mqtt.kafka-topic}",
      groupId = "${spring.kafka.consumer.group-id}-bsm-mqtt-depositor",
      concurrency = "${listen.concurrency:1}", containerFactory = "kafkaListenerContainerFactory")
  public void bsmDepositListener(String message) {
    try {
      LocalDateTime startTime = LocalDateTime.now(ZoneOffset.UTC);
      OdeBsmData msg = mapper.readValue(message, OdeBsmData.class);
      String odeReceivedAt = msg.getMetadata().getOdeReceivedAt();

      if (isMessageStale(odeReceivedAt)) {
        return;
      }

      byte[] asn1Bytes = Hex.decode(msg.getMetadata().getAsn1());

      J2735Bsm bsm = (J2735Bsm) msg.getPayload().getData();
      String topic = ImpMqttTopicBuilder.buildRegionalTopic(messageType,
          bsm.getCoreData().getPosition(), 7, impProperties);

      mqttService.publishAsn1Bytes(topic, asn1Bytes, false).exceptionally(throwable -> {
        log.error("Failed to publish BSM message: {}", throwable.getMessage());
        return null;
      });

      log.info("Sending BSM message to MQTT topics: {}", topic);
      recordLatency(odeReceivedAt, startTime);

    } catch (Exception e) {
      log.error("Error processing BSM message", e);
    }
  }
}
