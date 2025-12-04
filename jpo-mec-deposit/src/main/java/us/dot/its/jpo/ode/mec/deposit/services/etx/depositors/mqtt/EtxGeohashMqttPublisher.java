package us.dot.its.jpo.ode.mec.deposit.services.etx.depositors.mqtt;

import ch.hsr.geohash.GeoHash;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.bouncycastle.util.encoders.Hex;

import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttNamespace;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.GeoHashRoutedMsg;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.GeoRoutedMsg;
import us.dot.its.jpo.ode.mec.deposit.services.base.AbstractEtxMqttDepositor;
import us.dot.its.jpo.ode.mec.deposit.services.etx.EtxMqttPublishService;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttProtobufBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttTopicBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.MessageTypeDetector;

/**
 * Publisher class for handling geohash-routed messages via MQTT integration with ETX. This
 * publisher consumes GeoHashRoutedMsg protobuf messages from Kafka and publishes GeoRoutedMsg
 * protobuf messages to MQTT topics.
 */
@Component
@Slf4j
@ConditionalOnProperty(
    value = {"mec-deposit.etx.depositors.geohash.mqtt.enabled", "mec-deposit.etx.enabled"},
    havingValue = "true")
public class EtxGeohashMqttPublisher extends AbstractEtxMqttDepositor {

  public EtxGeohashMqttPublisher(MecDepositProperties mecDepositProperties,
      EtxProperties etxProperties, EtxMqttProperties mqttProperties,
      EtxMqttPublishService mqttService, MeterRegistry registry,
      KafkaTemplate<String, String> kafkaTemplate) {
    super(mecDepositProperties, etxProperties, mqttProperties, EtxMessageType.BSM, mqttService,
        registry, kafkaTemplate);
  }

  /**
   * Listens for GeoHashRoutedMsg protobuf messages from Kafka and publishes them as GeoRoutedMsg
   * protobuf messages to MQTT topics.
   *
   * @param geoHashRoutedMsgBytes The GeoHashRoutedMsg protobuf message bytes from Kafka
   */
  @Async("kafkaListenerExecutor")
  @KafkaListener(topics = "${mec-deposit.etx.depositors.geohash.mqtt.kafka-topic}",
      groupId = "${spring.kafka.consumer.group-id}-geohash-mqtt-publisher",
      concurrency = "${spring.kafka.listener.concurrency:1}",
      containerFactory = "byteArrayKafkaListenerContainerFactory")
  public void geohashPublishListener(byte[] geoHashRoutedMsgBytes) {
    boolean retain = false;
    Set<String> topicSet = null;
    String asn1Hex = "";
    LocalDateTime depositedAt = LocalDateTime.now(ZoneOffset.UTC);

    try {
      // Parse the incoming GeoHashRoutedMsg
      GeoHashRoutedMsg geoHashRoutedMsg = GeoHashRoutedMsg.parseFrom(geoHashRoutedMsgBytes);

      // Extract the original message bytes and geohash
      byte[] originalMessageBytes = geoHashRoutedMsg.getMsgBytes().toByteArray();
      String geohash = geoHashRoutedMsg.getGeohash();

      // Convert to hex for logging/metrics
      asn1Hex = Hex.toHexString(originalMessageBytes);

      // Build GeoRoutedMsg for MQTT publishing
      Instant timestamp = depositedAt.toInstant(ZoneOffset.UTC);

      // Determine namespace (retain -> RegionalStatic)
      EtxMqttNamespace namespace =
          retain ? EtxMqttNamespace.REGIONAL_STATIC : EtxMqttNamespace.REGIONAL;

      GeoHash geohashObject = GeoHash.fromGeohashString(geohash);
      double latitude = geohashObject.getOriginatingPoint().getLatitude();
      double longitude = geohashObject.getOriginatingPoint().getLongitude();
      // Build GeoRoutedMsg without position (since it won't be used by end users)
      GeoRoutedMsg geoRoutedMsg = EtxMqttProtobufBuilder.buildGeoRoutedMsg(originalMessageBytes,
          timestamp, latitude, longitude);
      byte[] geoRoutedMsgBytes = geoRoutedMsg.toByteArray();

      // If the payload is empty and retained is enabled, publish an empty message to clear retained
      // state
      // if (originalMessageBytes == null || originalMessageBytes.length == 0 || asn1Hex.isBlank())
      // {
      // // Use this.messageType when detection is not possible due to empty payload
      // geoRoutedMsgBytes = new byte[0];
      // }

      // Extract message type from the original message bytes
      EtxMessageType detectedMessageType =
          MessageTypeDetector.detectMessageType(originalMessageBytes);
      if (detectedMessageType == null) {
        detectedMessageType = EtxMessageType.TIM;
      }

      // Generate single topic based on geohash and detected message type
      // Use geohash directly for topic generation to avoid redundant coordinate conversion
      String topic = EtxMqttTopicBuilder.buildTopicFromGeohash(namespace, geohash,
          detectedMessageType, mqttProperties.getPrecision(), mqttProperties.getVendor(),
          mqttProperties.getMessageFormat(), etxProperties.getClientType(),
          etxProperties.getClientSubType());
      topicSet = Set.of(topic);

      mqttService.publishAsn1Bytes(topic, geoRoutedMsgBytes, retain);
      log.debug("Sending GeoRoutedMsg to MQTT topic: {} (detected type: {}, geohash: {})", topic,
          detectedMessageType, geohash);

      handleProcessingSuccess(topicSet, timestamp.toString(), depositedAt, asn1Hex);
    } catch (Exception e) {
      handleProcessingError(e, topicSet, depositedAt.toInstant(ZoneOffset.UTC).toEpochMilli(),
          asn1Hex);
    }
  }

}
