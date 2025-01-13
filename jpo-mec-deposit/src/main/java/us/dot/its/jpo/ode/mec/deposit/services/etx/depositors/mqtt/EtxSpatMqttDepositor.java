package us.dot.its.jpo.ode.mec.deposit.services.etx.depositors.mqtt;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.GeoRoutedMsg;
import us.dot.its.jpo.ode.mec.deposit.services.base.AbstractEtxMqttDepositor;
import us.dot.its.jpo.ode.mec.deposit.services.etx.EtxMqttPublishService;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxDepositMetrics;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;
import us.dot.its.jpo.ode.mec.deposit.utils.MapRefPointCollector;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttProtobufBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttTopicBuilder;
import us.dot.its.jpo.ode.model.OdeSpatData;
import us.dot.its.jpo.ode.plugin.j2735.J2735SPAT;

/**
 * Depositor class for handling SPAT messages via MQTT integration with ETX.
 */
@Component
@Slf4j
@ConditionalOnProperty(
    value = {"mec-deposit.etx.depositors.spat.mqtt.enabled", "mec-deposit.etx.enabled"},
    havingValue = "true")
public class EtxSpatMqttDepositor extends AbstractEtxMqttDepositor {

  @Autowired
  private MapRefPointCollector mapDataCollector;

  public EtxSpatMqttDepositor(MecDepositProperties mecDepositProperties,
      EtxProperties etxProperties, EtxMqttPublishService mqttService, MeterRegistry registry,
      KafkaTemplate<String, String> kafkaTemplate) {
    super(mecDepositProperties, etxProperties, EtxMessageType.SPAT, mqttService, registry,
        kafkaTemplate);
  }

  /**
   * Listens for SPAT messages from Kafka and deposits them to appropriate MQTT topics.
   *
   * @param message The SPAT message from Kafka in JSON format
   */
  @Async("kafkaListenerExecutor")
  @KafkaListener(topics = "${mec-deposit.etx.depositors.spat.mqtt.kafka-topic}",
      groupId = "${spring.kafka.consumer.group-id}-spat-mqtt-depositor",
      concurrency = "${listen.concurrency:1}", containerFactory = "kafkaListenerContainerFactory")
  public void spatDepositListener(String message) {
    boolean retain = false;
    Set<String> topicSet = null;
    String odeReceivedAt = null;
    try {
      OdeSpatData msg = mapper.readValue(message, OdeSpatData.class);
      odeReceivedAt = msg.getMetadata().getOdeReceivedAt();

      if (isMessageStale(odeReceivedAt)) {
        return;
      }

      byte[] messageBytes = Hex.decode(msg.getMetadata().getAsn1());
      J2735SPAT spatMsg = (J2735SPAT) msg.getPayload().getData();

      // If the message format is J2735_GR, we need to convert the message to a GeoRoutedMsg
      if (etxProperties.getMqtt().getMessageFormat() == EtxMqttMessageFormat.J2735_GR) {
        Instant timestamp = Instant.parse(odeReceivedAt);
        GeoRoutedMsg geoRoutedMsg =
            EtxMqttProtobufBuilder.buildGeoRoutedMsg(messageBytes, timestamp);
        messageBytes = geoRoutedMsg.toByteArray();
      }

      topicSet = EtxMqttTopicBuilder.getSpatTopicList(spatMsg, etxProperties, mapDataCollector);

      for (String topic : topicSet) {
        mqttService.publishAsn1Bytes(topic, messageBytes, retain);
        log.info("Successfully sent SPaT message to MQTT topic: {}", topic);
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
      log.error("Error processing SPaT message", e);

      // Publish failure metrics with the attempted topic if available
      publishMetrics(EtxDepositMetrics.builder().depositorType(getDepositorType())
          .messageType(messageType).odeReceivedAt(odeReceivedAt)
          .depositedAt(LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME))
          .success(false).errorMessage(errorMessage).topics(topicSet != null ? topicSet : null)
          .build());
    }
  }
}
