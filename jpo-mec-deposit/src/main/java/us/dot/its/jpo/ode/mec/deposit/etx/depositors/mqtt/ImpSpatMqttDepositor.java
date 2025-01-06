package us.dot.its.jpo.ode.mec.deposit.etx.depositors.mqtt;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.models.mqtt.EtxMqttMessageType;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttService;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttTopicBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.MapRefPointCollector;
import us.dot.its.jpo.ode.model.OdeSpatData;
import us.dot.its.jpo.ode.plugin.j2735.J2735SPAT;

/**
 * Depositor class for handling SPAT messages via MQTT integration with ETX.
 */
@Component
@Slf4j
@ConditionalOnProperty(value = {"etx.depositors.spat.mqtt.enabled", "etx.enabled"},
    havingValue = "true")
public class ImpSpatMqttDepositor extends AbstractEtxMqttDepositor {

  @Autowired
  private MapRefPointCollector mapDataCollector;

  public ImpSpatMqttDepositor(EtxProperties etxProperties, EtxMqttService mqttService,
      MeterRegistry registry) {
    super(etxProperties, mqttService, EtxMqttMessageType.SPAT, registry);
  }

  /**
   * Listens for SPAT messages from Kafka and deposits them to appropriate MQTT topics.
   *
   * @param message The SPAT message from Kafka in JSON format
   */
  @Async("kafkaListenerExecutor")
  @KafkaListener(topics = "${etx.depositors.spat.mqtt.kafka-topic}",
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
      Set<String> topicList =
          EtxMqttTopicBuilder.getSpatTopicList(spatMsg, etxProperties, mapDataCollector);

      byte[] asn1Bytes = Hex.decode(asn1String);

      for (String topic : topicList) {
        mqttService.publishAsn1Bytes(topic, asn1Bytes, retain);
        log.info("Sending SPAT message to MQTT topics: {}", topic);
      }

      recordLatency(odeReceivedAt, startTime);

    } catch (Exception e) {
      log.error("Error processing SPaT message", e);
    }
  }
}
