package us.dot.its.jpo.ode.mec.deposit.etx.depositors.mqtt;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.GeoRoutedMsg;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxDepositMetrics;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;
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
@ConditionalOnProperty(
    value = {"mec-deposit.etx.depositors.tim.mqtt.enabled", "mec-deposit.etx.enabled"},
    havingValue = "true")
public class EtxTimMqttDepositor extends AbstractEtxMqttDepositor {

  public EtxTimMqttDepositor(MecDepositProperties mecDepositProperties, EtxProperties etxProperties,
      EtxMqttService mqttService, MeterRegistry registry,
      KafkaTemplate<String, String> kafkaTemplate) {
    super(mecDepositProperties, etxProperties, EtxMessageType.TIM, mqttService, registry,
        kafkaTemplate);
  }

  /**
   * Listens for TIM messages from Kafka and deposits them to appropriate MQTT topics.
   *
   * @param message The TIM message from Kafka in JSON format
   */
  @Async("kafkaListenerExecutor")
  @KafkaListener(topics = "${mec-deposit.etx.depositors.tim.mqtt.kafka-topic}",
      groupId = "${spring.kafka.consumer.group-id}-tim-mqtt-depositor",
      concurrency = "${listen.concurrency:1}", containerFactory = "kafkaListenerContainerFactory")
  public void timDepositListener(String message) {
    boolean retain = false;
    Set<String> topicSet = null;
    String odeReceivedAt = null;
    try {
      OdeTimData msg = mapper.readValue(message, OdeTimData.class);
      odeReceivedAt = msg.getMetadata().getOdeReceivedAt();

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
      topicSet = EtxMqttTopicBuilder.getTimTopicList(dataFramesList, etxProperties);

      for (String topic : topicSet) {
        mqttService.publishAsn1Bytes(topic, messageBytes, retain);
        log.info("Sending TIM message to MQTT topics: {}", topic);
      }

      LocalDateTime depositedAt = LocalDateTime.now(ZoneOffset.UTC);

      // Only publish success metrics after confirmed MQTT publish
      publishMetrics(EtxDepositMetrics.builder().depositorType(getDepositorType())
          .messageType(messageType).odeReceivedAt(odeReceivedAt)
          .depositedAt(depositedAt.format(DateTimeFormatter.ISO_DATE_TIME))
          .latencyMs(recordLatency(odeReceivedAt, depositedAt).toMillis()).success(true)
          .topics(topicSet).build());
    } catch (Exception e) {
      String errorMessage = e.getMessage();
      log.error("Error processing TIM message", e);

      // Publish failure metrics with the attempted topic if available
      publishMetrics(EtxDepositMetrics.builder().depositorType(getDepositorType())
          .messageType(messageType).odeReceivedAt(odeReceivedAt)
          .depositedAt(LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME))
          .success(false).errorMessage(errorMessage).topics(topicSet != null ? topicSet : null)
          .build());
    }
  }
}

