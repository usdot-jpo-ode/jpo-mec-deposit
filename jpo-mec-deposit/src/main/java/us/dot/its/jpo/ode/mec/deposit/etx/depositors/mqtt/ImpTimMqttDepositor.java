package us.dot.its.jpo.ode.mec.deposit.etx.depositors.mqtt;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Set;
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
import us.dot.its.jpo.ode.model.OdeTimData;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.TravelerInformation;

/**
 * Depositor class for handling TIM messages via MQTT integration with ETX.
 */
@Component
@Slf4j
@ConditionalOnProperty(value = {"etx.depositors.tim.mqtt.enabled", "etx.enabled"},
    havingValue = "true")
public class ImpTimMqttDepositor extends AbstractEtxMqttDepositor {

  public ImpTimMqttDepositor(EtxProperties etxProperties, EtxMqttService mqttService,
      MeterRegistry registry) {
    super(etxProperties, mqttService, EtxMqttMessageType.TIM, registry);
  }

  /**
   * Listens for TIM messages from Kafka and deposits them to appropriate MQTT topics.
   *
   * @param message The TIM message from Kafka in JSON format
   */
  @Async("kafkaListenerExecutor")
  @KafkaListener(topics = "${etx.depositors.tim.mqtt.kafka-topic}",
      groupId = "${spring.kafka.consumer.group-id}-tim-mqtt-depositor",
      concurrency = "${listen.concurrency:1}", containerFactory = "kafkaListenerContainerFactory")
  public void timDepositListener(String message) {
    boolean retain = false;
    try {
      final LocalDateTime startTime = LocalDateTime.now(ZoneOffset.UTC);
      OdeTimData msg = mapper.readValue(message, OdeTimData.class);

      String odeReceivedAt = msg.getMetadata().getOdeReceivedAt();
      if (isMessageStale(odeReceivedAt)) {
        return;
      }

      byte[] messageBytes = Hex.decode(msg.getMetadata().getAsn1());

      var timMsg = (TravelerInformation) msg.getPayload().getData();

      // If the message format is J2735_GR, we need to convert the message to a GeoRoutedMsg
      if (etxProperties.getMqtt().getMessageFormat() == EtxMqttMessageFormat.J2735_GR) {
        Instant timestamp = Instant.parse(odeReceivedAt);
        GeoRoutedMsg geoRoutedMsg =
            EtxMqttProtobufBuilder.buildGeoRoutedMsg(messageBytes, timestamp);
        messageBytes = geoRoutedMsg.toByteArray();
      }

      var dataFramesList = timMsg.getDataFrames();
      Set<String> topicList = EtxMqttTopicBuilder.getTimTopicList(dataFramesList, etxProperties);

      for (String topic : topicList) {
        mqttService.publishAsn1Bytes(topic, messageBytes, retain);
        log.info("Sending TIM message to MQTT topics: {}", topic);
      }

      recordLatency(odeReceivedAt, startTime);

    } catch (

    Exception e) {
      log.error("Error processing TIM message", e);
    }
  }
}

