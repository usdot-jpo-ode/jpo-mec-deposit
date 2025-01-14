package us.dot.its.jpo.ode.mec.deposit.services.etx.depositors.mqtt;

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
import us.dot.its.jpo.ode.mec.deposit.services.base.AbstractEtxMqttDepositor;
import us.dot.its.jpo.ode.mec.deposit.services.etx.EtxMqttPublishService;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxDepositMetrics;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttProtobufBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttTopicBuilder;
import us.dot.its.jpo.ode.model.OdeBsmData;
import us.dot.its.jpo.ode.plugin.j2735.J2735Bsm;
import us.dot.its.jpo.ode.plugin.j2735.OdePosition3D;

/**
 * Depositor class for handling BSM messages via MQTT integration with ETX.
 */
@Component
@Slf4j
@ConditionalOnProperty(
    value = {"mec-deposit.etx.depositors.bsm.mqtt.enabled", "mec-deposit.etx.enabled"},
    havingValue = "true")
public class EtxBsmMqttDepositor extends AbstractEtxMqttDepositor {

  public EtxBsmMqttDepositor(MecDepositProperties mecDepositProperties, EtxProperties etxProperties,
      EtxMqttProperties mqttProperties, EtxMqttPublishService mqttService, MeterRegistry registry,
      KafkaTemplate<String, String> kafkaTemplate) {
    super(mecDepositProperties, etxProperties, mqttProperties, EtxMessageType.BSM, mqttService,
        registry, kafkaTemplate);
  }

  /**
   * Listens for BSM messages from Kafka and deposits them to appropriate MQTT topics.
   *
   * @param message The BSM message from Kafka in JSON format
   */
  @Async("kafkaListenerExecutor")
  @KafkaListener(topics = "${mec-deposit.etx.depositors.bsm.mqtt.kafka-topic}",
      groupId = "${spring.kafka.consumer.group-id}-bsm-mqtt-depositor",
      concurrency = "${listen.concurrency:1}", containerFactory = "kafkaListenerContainerFactory")
  public void bsmDepositListener(String message) {
    boolean retain = false;
    String topic = null;
    String odeReceivedAt = null;
    try {
      OdeBsmData msg = mapper.readValue(message, OdeBsmData.class);
      odeReceivedAt = msg.getMetadata().getOdeReceivedAt();

      if (isMessageStale(odeReceivedAt)) {
        return;
      }

      byte[] messageBytes = Hex.decode(msg.getMetadata().getAsn1());

      J2735Bsm bsm = (J2735Bsm) msg.getPayload().getData();
      OdePosition3D refPoint = bsm.getCoreData().getPosition();

      // If the message format is J2735_GR, we need to convert the message to a GeoRoutedMsg
      if (mqttProperties.getMessageFormat() == EtxMqttMessageFormat.J2735_GR) {
        Instant timestamp = Instant.parse(odeReceivedAt);
        GeoRoutedMsg geoRoutedMsg = EtxMqttProtobufBuilder.buildGeoRoutedMsg(messageBytes,
            timestamp, refPoint.getLatitude().doubleValue(), refPoint.getLongitude().doubleValue());
        messageBytes = geoRoutedMsg.toByteArray();
      }

      topic =
          EtxMqttTopicBuilder.buildRegionalTopic(messageType, refPoint.getLatitude().doubleValue(),
              refPoint.getLongitude().doubleValue(), mqttProperties.getPrecision(),
              mqttProperties.getVendor(), mqttProperties.getMessageFormat(),
              etxProperties.getClientType(), etxProperties.getClientSubType());

      // This will now block until the message is published or throws an exception
      mqttService.publishAsn1Bytes(topic, messageBytes, retain);
      log.info("Successfully sent BSM message to MQTT topic: {}", topic);
      LocalDateTime depositedAt = LocalDateTime.now(ZoneOffset.UTC);

      // Only publish success metrics after confirmed MQTT publish
      publishMetrics(EtxDepositMetrics.builder().depositorType(getDepositorType())
          .messageType(messageType).odeReceivedAt(odeReceivedAt)
          .depositedAt(depositedAt.format(DateTimeFormatter.ISO_DATE_TIME))
          .latencyMs(recordLatency(odeReceivedAt, depositedAt).toMillis()).success(true)
          .topics(Set.of(topic)).build());
    } catch (Exception e) {
      String errorMessage = e.getMessage();
      log.error("Error processing BSM message", e);

      // Publish failure metrics with the attempted topic if available
      publishMetrics(EtxDepositMetrics.builder().depositorType(getDepositorType())
          .messageType(messageType).odeReceivedAt(odeReceivedAt)
          .depositedAt(LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME))
          .success(false).errorMessage(errorMessage).topics(topic != null ? Set.of(topic) : null)
          .build());
      errorCounter.increment();
    }
  }
}
