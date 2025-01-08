package us.dot.its.jpo.ode.mec.deposit.etx.depositors.mqtt;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.GeoRoutedMsg;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.models.mqtt.EtxMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.etx.models.mqtt.EtxMqttMessageType;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProtobufBuilder;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttService;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttTopicBuilder;
import us.dot.its.jpo.ode.model.OdeBsmData;
import us.dot.its.jpo.ode.plugin.j2735.J2735Bsm;
import us.dot.its.jpo.ode.plugin.j2735.OdePosition3D;

/**
 * Depositor class for handling BSM messages via MQTT integration with ETX.
 */
@Component
@Slf4j
@ConditionalOnProperty(value = {"etx.depositors.bsm.mqtt.enabled", "etx.enabled"},
    havingValue = "true")
public class ImpBsmMqttDepositor extends AbstractEtxMqttDepositor {

  public ImpBsmMqttDepositor(EtxProperties etxProperties, EtxMqttService mqttService,
      MeterRegistry registry) {
    super(etxProperties, mqttService, EtxMqttMessageType.BSM, registry);
  }

  /**
   * Listens for BSM messages from Kafka and deposits them to appropriate MQTT topics.
   *
   * @param message The BSM message from Kafka in JSON format
   */
  @Async("kafkaListenerExecutor")
  @KafkaListener(topics = "${etx.depositors.bsm.mqtt.kafka-topic}",
      groupId = "${spring.kafka.consumer.group-id}-bsm-mqtt-depositor",
      concurrency = "${listen.concurrency:1}", containerFactory = "kafkaListenerContainerFactory")
  public void bsmDepositListener(String message) {
    try {
      final LocalDateTime startTime = LocalDateTime.now(ZoneOffset.UTC);
      OdeBsmData msg = mapper.readValue(message, OdeBsmData.class);
      String odeReceivedAt = msg.getMetadata().getOdeReceivedAt();

      if (isMessageStale(odeReceivedAt)) {
        return;
      }

      byte[] messageBytes = Hex.decode(msg.getMetadata().getAsn1());

      J2735Bsm bsm = (J2735Bsm) msg.getPayload().getData();
      OdePosition3D refPoint = bsm.getCoreData().getPosition();

      // If the message format is J2735_GR, we need to convert the message to a GeoRoutedMsg
      if (etxProperties.getMqtt().getMessageFormat() == EtxMqttMessageFormat.J2735_GR) {
        Instant timestamp = Instant.parse(odeReceivedAt);
        GeoRoutedMsg geoRoutedMsg = EtxMqttProtobufBuilder.buildGeoRoutedMsg(messageBytes,
            timestamp, refPoint.getLatitude().doubleValue(), refPoint.getLongitude().doubleValue());
        messageBytes = geoRoutedMsg.toByteArray();
      }

      String topic = EtxMqttTopicBuilder.buildRegionalTopic(messageType,
          bsm.getCoreData().getPosition(), 7, etxProperties);

      mqttService.publishAsn1Bytes(topic, messageBytes, false).exceptionally(throwable -> {
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
