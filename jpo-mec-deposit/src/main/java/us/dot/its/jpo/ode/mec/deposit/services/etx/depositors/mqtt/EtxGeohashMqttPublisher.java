package us.dot.its.jpo.ode.mec.deposit.services.etx.depositors.mqtt;

import ch.hsr.geohash.GeoHash;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.bouncycastle.util.encoders.Hex;

import us.dot.its.jpo.ode.mec.deposit.config.condition.ConditionalOnAnyMqttBrokerMqttDepositor;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.BrokerPublishPayload;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttNamespace;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.GeoHashRoutedMsg;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttBrokerTarget;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttFanoutPublishResult;
import us.dot.its.jpo.ode.mec.deposit.services.base.AbstractEtxMqttDepositor;
import us.dot.its.jpo.ode.mec.deposit.services.etx.EtxMqttPublishService;
import us.dot.its.jpo.ode.mec.deposit.services.etx.mqtt.EtxBrokerPublisher;
import us.dot.its.jpo.ode.mec.deposit.services.mqtt.MultiBrokerPublishService;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttProtobufBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttTopicBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.NmiMqttTopicBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.MessageTypeDetector;

/**
 * Publisher class for handling geohash-routed messages via MQTT integration with ETX. This
 * publisher consumes GeoHashRoutedMsg protobuf messages from Kafka. ETX may publish a
 * GeoHashRoutedMsg wire form when {@code j2735_gr} is configured; NMI/AV always receive the inner
 * ASN.1 bytes only.
 */
@Component
@Slf4j
@ConditionalOnAnyMqttBrokerMqttDepositor("geohash")
public class EtxGeohashMqttPublisher extends AbstractEtxMqttDepositor {
  private final MultiBrokerPublishService multiBrokerPublishService;

  /**
   * Primary Spring constructor.
   */
  @Autowired
  public EtxGeohashMqttPublisher(MecDepositProperties mecDepositProperties,
      EtxProperties etxProperties, EtxMqttProperties mqttProperties,
      @Nullable EtxMqttPublishService mqttService, MeterRegistry registry,
      MultiBrokerPublishService multiBrokerPublishService,
      KafkaTemplate<String, String> kafkaTemplate) {
    super(mecDepositProperties, etxProperties, mqttProperties, EtxMessageType.BSM, mqttService,
        registry, kafkaTemplate);
    this.multiBrokerPublishService = multiBrokerPublishService;
  }

  /**
   * Backward-compatible constructor used by unit tests.
   */
  public EtxGeohashMqttPublisher(MecDepositProperties mecDepositProperties, EtxProperties etxProperties,
      EtxMqttProperties mqttProperties, EtxMqttPublishService mqttService, MeterRegistry registry,
      KafkaTemplate<String, String> kafkaTemplate) {
    this(mecDepositProperties, etxProperties, mqttProperties, mqttService, registry,
        new MultiBrokerPublishService(List.of(new EtxBrokerPublisher(mqttService)), mqttProperties,
            registry),
        kafkaTemplate);
  }

  /**
   * Listens for GeoHashRoutedMsg protobuf messages from Kafka and fans out to MQTT brokers.
   *
   * @param geoHashRoutedMsgBytes The GeoHashRoutedMsg protobuf message bytes from Kafka
   */
  @KafkaListener(topics = "${mec-deposit.etx.mqtt-brokers.etx.depositors.geohash.mqtt.kafka-topic}",
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

      // Determine namespace (retain -> RegionalStatic)
      EtxMqttNamespace namespace =
          retain ? EtxMqttNamespace.REGIONAL_STATIC : EtxMqttNamespace.REGIONAL;

      final GeoHash geohashObject = GeoHash.fromGeohashString(geohash);
      // Extract message type from the original message bytes
      EtxMessageType detectedMessageType =
          MessageTypeDetector.detectMessageType(originalMessageBytes);
      if (detectedMessageType == null) {
        detectedMessageType = EtxMessageType.TIM;
      }

      // Generate single topic based on geohash and detected message type
      // Use geohash directly for topic generation to avoid redundant coordinate conversion
      String etxTopic = EtxMqttTopicBuilder.buildTopicFromGeohash(namespace, geohash, detectedMessageType,
          mqttProperties.getPrecision(), mqttProperties.getVendor(), mqttProperties.getMessageFormat(),
          etxProperties.getClientType(), etxProperties.getClientSubType());
      String dsrcMsgId = MessageTypeDetector.getDsrcMsgIdForMessageType(detectedMessageType);
      String nmiTopic = null;
      String avTopic = null;
      if (multiBrokerPublishService.isTargetActive(MqttBrokerTarget.NMI)
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.NMI, "geohash")) {
        nmiTopic = NmiMqttTopicBuilder.buildTopicFromGeohash(geohash,
            etxProperties.mqttTopicPrecision(MqttBrokerTarget.NMI),
            dsrcMsgId);
      }
      if (multiBrokerPublishService.isTargetActive(MqttBrokerTarget.AV)
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.AV, "geohash")) {
        avTopic = NmiMqttTopicBuilder.buildTopicFromGeohash(geohash,
            etxProperties.mqttTopicPrecision(MqttBrokerTarget.AV),
            dsrcMsgId);
      }
      Instant depositedInstant = depositedAt.toInstant(ZoneOffset.UTC);
      byte[] etxPayload = EtxMqttProtobufBuilder.toEtxMqttWirePayloadGeoHash(originalMessageBytes,
          mqttProperties.getMessageFormat(), depositedInstant, geohash);
      Map<MqttBrokerTarget, BrokerPublishPayload> payloads = new EnumMap<>(MqttBrokerTarget.class);
      if (multiBrokerPublishService.isTargetActive(MqttBrokerTarget.ETX)
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.ETX, "geohash")) {
        payloads.put(MqttBrokerTarget.ETX, BrokerPublishPayload.builder().target(MqttBrokerTarget.ETX)
            .topics(Set.of(etxTopic)).payload(etxPayload).build());
      }
      if (nmiTopic != null) {
        payloads.put(MqttBrokerTarget.NMI, BrokerPublishPayload.builder().target(MqttBrokerTarget.NMI)
            .topics(Set.of(nmiTopic)).payload(originalMessageBytes).build());
      }
      if (avTopic != null) {
        payloads.put(MqttBrokerTarget.AV, BrokerPublishPayload.builder().target(MqttBrokerTarget.AV)
            .topics(Set.of(avTopic)).payload(originalMessageBytes).build());
      }
      topicSet = MultiBrokerPublishService.unionPayloadTopics(payloads);
      MqttFanoutPublishResult fanout = multiBrokerPublishService.publish(payloads, retain);
      Set<String> metricTopics =
          fanout.publishedTopics().isEmpty() ? topicSet : fanout.publishedTopics();
      log.debug("Sending signed payload to ETX/NMI topics: {} / {} (detected type: {}, geohash: {}, lat: {}, lon: {})",
          etxTopic, nmiTopic != null ? nmiTopic : "-", detectedMessageType, geohash,
          geohashObject.getOriginatingPoint().getLatitude(),
          geohashObject.getOriginatingPoint().getLongitude());

      Instant timestamp = depositedAt.toInstant(ZoneOffset.UTC);
      handleProcessingSuccess(metricTopics, timestamp.toString(), depositedAt, asn1Hex,
          fanout.mqttBrokerTargets().isEmpty() ? null : fanout.mqttBrokerTargets());
    } catch (Exception e) {
      handleProcessingError(e, topicSet, depositedAt.toInstant(ZoneOffset.UTC).toEpochMilli(),
          asn1Hex);
    }
  }

}
