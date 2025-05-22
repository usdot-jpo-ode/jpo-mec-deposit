package us.dot.its.jpo.ode.mec.deposit.services.etx.depositors.mqtt;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.GeoRoutedMsg;
import us.dot.its.jpo.ode.mec.deposit.services.base.AbstractEtxMqttDepositor;
import us.dot.its.jpo.ode.mec.deposit.services.etx.EtxMqttPublishService;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttProtobufBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttTopicBuilder;
import us.dot.its.jpo.asn.j2735.r2024.Common.Position3D;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;
import us.dot.its.jpo.asn.j2735.r2024.SensorDataSharingMessage.SensorDataSharingMessage;

/**
 * Depositor class for handling SDSM messages via MQTT integration with ETX.
 */
@Component
@Slf4j
@ConditionalOnProperty(
    value = {"mec-deposit.etx.depositors.sdsm.mqtt.enabled", "mec-deposit.etx.enabled"},
    havingValue = "true")
public class EtxSdsmMqttDepositor extends AbstractEtxMqttDepositor {

  private static final double MICRODEGREES_TO_DECIMAL_DEGREES_CONVERSION_FACTOR = 1.0 / 10000000.0;

  public EtxSdsmMqttDepositor(MecDepositProperties mecDepositProperties,
      EtxProperties etxProperties, EtxMqttProperties mqttProperties,
      EtxMqttPublishService mqttService, MeterRegistry registry,
      KafkaTemplate<String, String> kafkaTemplate) {
    super(mecDepositProperties, etxProperties, mqttProperties, EtxMessageType.SDSM, mqttService,
        registry, kafkaTemplate);
  }

  /**
   * Listens for SDSM messages from Kafka and deposits them to appropriate MQTT topics.
   *
   * @param message The SDSM message from Kafka in JSON format
   */
  @Async("kafkaListenerExecutor")
  @KafkaListener(topics = "${mec-deposit.etx.depositors.sdsm.mqtt.kafka-topic}",
      groupId = "${spring.kafka.consumer.group-id}-sdsm-mqtt-depositor",
      concurrency = "${spring.kafka.listener.concurrency:1}",
      containerFactory = "kafkaListenerContainerFactory")
  public void sdsmDepositListener(String message) {
    boolean retain = false;
    String topic = null;
    String odeReceivedAt = null;
    String asn1Hex = "";
    try {
      OdeMessageFrameData msg = mapper.readValue(message, OdeMessageFrameData.class);
      odeReceivedAt = msg.getMetadata().getOdeReceivedAt();
      asn1Hex = msg.getMetadata().getAsn1();

      if (isMessageStale(odeReceivedAt)) {
        return;
      }

      byte[] messageBytes = Hex.decode(msg.getMetadata().getAsn1());

      SensorDataSharingMessage sdsm =
          (SensorDataSharingMessage) msg.getPayload().getData().getValue();
      Position3D refPoint = sdsm.getRefPos();
      LocalDateTime depositedAt = LocalDateTime.now(ZoneOffset.UTC);

      // Convert lat/long from 1/10th microdegrees to decimal degrees
      Double latitude =
          (double) refPoint.getLat().getValue() * MICRODEGREES_TO_DECIMAL_DEGREES_CONVERSION_FACTOR;
      Double longitude = (double) refPoint.getLong_().getValue()
          * MICRODEGREES_TO_DECIMAL_DEGREES_CONVERSION_FACTOR;

      // If the message format is J2735_GR, we need to convert the message to a
      // GeoRoutedMsg
      if (mqttProperties.getMessageFormat() == EtxMqttMessageFormat.J2735_GR) {
        Instant timestamp = depositedAt.toInstant(ZoneOffset.UTC);
        GeoRoutedMsg geoRoutedMsg =
            EtxMqttProtobufBuilder.buildGeoRoutedMsg(messageBytes, timestamp, latitude, longitude);
        messageBytes = geoRoutedMsg.toByteArray();
      }

      topic = EtxMqttTopicBuilder.buildRegionalTopic(messageType, latitude, longitude,
          mqttProperties.getPrecision(), mqttProperties.getVendor(),
          mqttProperties.getMessageFormat(), etxProperties.getClientType(),
          etxProperties.getClientSubType());

      // This will now block until the message is published or throws an exception
      mqttService.publishAsn1Bytes(topic, messageBytes, retain);
      log.debug("Successfully sent SDSM message to MQTT topic: {}", topic);

      handleProcessingSuccess(Set.of(topic), null, odeReceivedAt, depositedAt, asn1Hex);
    } catch (Exception e) {
      handleProcessingError(e, Set.of(topic), null,
          odeReceivedAt != null ? Instant.parse(odeReceivedAt).toEpochMilli() : 0, asn1Hex);
    }
  }
}
