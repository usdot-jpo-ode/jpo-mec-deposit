package us.dot.its.jpo.ode.mec.deposit.services.depositors.mqtt;

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
import org.bouncycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.config.condition.ConditionalOnAnyMqttBrokerMqttDepositor;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.BrokerPublishPayload;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttNamespace;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.GeoHashRoutedMsg;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttBrokerTarget;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttFanoutPublishResult;
import us.dot.its.jpo.ode.mec.deposit.services.base.AbstractMqttDepositor;
import us.dot.its.jpo.ode.mec.deposit.services.etx.mqtt.EtxBrokerPublisher;
import us.dot.its.jpo.ode.mec.deposit.services.etx.mqtt.EtxMqttPublishService;
import us.dot.its.jpo.ode.mec.deposit.services.mqtt.MultiBrokerPublishService;
import us.dot.its.jpo.ode.mec.deposit.utils.MessageTypeDetector;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttProtobufBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttTopicBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.NmiMqttTopicBuilder;

/**
 * Publisher for handling geohash-routed messages via multi-broker MQTT fanout. Consumes
 * {@link GeoHashRoutedMsg} protobuf messages from Kafka, converts the geohash to lat/lon
 * coordinates, and fans out to configured MQTT brokers.
 */
@Component
@Slf4j
@ConditionalOnAnyMqttBrokerMqttDepositor("geohash")
public class GeohashMqttPublisher extends AbstractMqttDepositor {
  private final MultiBrokerPublishService multiBrokerPublishService;

  /**
   * Primary Spring constructor.
   */
  @Autowired
  public GeohashMqttPublisher(MecDepositProperties mecDepositProperties,
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
  public GeohashMqttPublisher(MecDepositProperties mecDepositProperties,
      EtxProperties etxProperties, EtxMqttProperties mqttProperties,
      EtxMqttPublishService mqttService, MeterRegistry registry,
      KafkaTemplate<String, String> kafkaTemplate) {
    this(mecDepositProperties, etxProperties, mqttProperties, mqttService, registry,
        new MultiBrokerPublishService(List.of(new EtxBrokerPublisher(mqttService)),
            Set.of(MqttBrokerTarget.ETX), registry),
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
      GeoHashRoutedMsg geoHashRoutedMsg = GeoHashRoutedMsg.parseFrom(geoHashRoutedMsgBytes);

      byte[] originalMessageBytes = geoHashRoutedMsg.getMsgBytes().toByteArray();
      String geohash = geoHashRoutedMsg.getGeohash();

      asn1Hex = Hex.toHexString(originalMessageBytes);

      EtxMqttNamespace namespace =
          retain ? EtxMqttNamespace.REGIONAL_STATIC : EtxMqttNamespace.REGIONAL;

      final GeoHash geohashObject = GeoHash.fromGeohashString(geohash);
      EtxMessageType detectedMessageType =
          MessageTypeDetector.detectMessageType(originalMessageBytes);
      if (detectedMessageType == null) {
        detectedMessageType = EtxMessageType.TIM;
      }

      String dsrcMsgId = MessageTypeDetector.getDsrcMsgIdForMessageType(detectedMessageType);
      String nmiTopic = null;
      String avTopic = null;
      String mbTopic = null;
      if (multiBrokerPublishService.isTargetActive(MqttBrokerTarget.NMI)
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.NMI, "geohash")) {
        nmiTopic = NmiMqttTopicBuilder.buildTopicFromGeohash(geohash,
            etxProperties.mqttTopicPrecision(MqttBrokerTarget.NMI), dsrcMsgId);
      }
      if (multiBrokerPublishService.isTargetActive(MqttBrokerTarget.AV)
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.AV, "geohash")) {
        avTopic = NmiMqttTopicBuilder.buildTopicFromGeohash(geohash,
            etxProperties.mqttTopicPrecision(MqttBrokerTarget.AV), dsrcMsgId);
      }
      if (multiBrokerPublishService.isTargetActive(MqttBrokerTarget.MB)
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.MB, "geohash")) {
        mbTopic = NmiMqttTopicBuilder.buildTopicFromGeohash(geohash,
            etxProperties.mqttTopicPrecision(MqttBrokerTarget.MB), dsrcMsgId);
      }
      Instant depositedInstant = depositedAt.toInstant(ZoneOffset.UTC);
      double latitude = geohashObject.getOriginatingPoint().getLatitude();
      double longitude = geohashObject.getOriginatingPoint().getLongitude();
      byte[] etxPayload = EtxMqttProtobufBuilder.toEtxMqttWirePayload(originalMessageBytes,
          mqttProperties.getMessageFormat(), depositedInstant, latitude, longitude);
      Map<MqttBrokerTarget, BrokerPublishPayload> payloads = new EnumMap<>(MqttBrokerTarget.class);
      String etxTopic = EtxMqttTopicBuilder.buildTopicFromGeohash(namespace, geohash,
          detectedMessageType, mqttProperties.getPrecision(), mqttProperties.getVendor(),
          mqttProperties.getMessageFormat(), etxProperties.getClientType(),
          etxProperties.getClientSubType());
      if (multiBrokerPublishService.isTargetActive(MqttBrokerTarget.ETX)
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.ETX, "geohash")) {
        payloads.put(MqttBrokerTarget.ETX, BrokerPublishPayload.builder()
            .target(MqttBrokerTarget.ETX).topics(Set.of(etxTopic)).payload(etxPayload).build());
      }
      if (nmiTopic != null) {
        payloads.put(MqttBrokerTarget.NMI,
            BrokerPublishPayload.builder().target(MqttBrokerTarget.NMI).topics(Set.of(nmiTopic))
                .payload(originalMessageBytes).build());
      }
      if (avTopic != null) {
        payloads.put(MqttBrokerTarget.AV, BrokerPublishPayload.builder().target(MqttBrokerTarget.AV)
            .topics(Set.of(avTopic)).payload(originalMessageBytes).build());
      }
      if (mbTopic != null) {
        payloads.put(MqttBrokerTarget.MB, BrokerPublishPayload.builder().target(MqttBrokerTarget.MB)
            .topics(Set.of(mbTopic)).payload(originalMessageBytes).build());
      }
      topicSet = MultiBrokerPublishService.unionPayloadTopics(payloads);
      MqttFanoutPublishResult fanout = multiBrokerPublishService.publish(payloads, retain);
      Set<String> metricTopics =
          fanout.publishedTopics().isEmpty() ? topicSet : fanout.publishedTopics();
      log.debug(
          "Sending signed payload to ETX/NMI topics: {} / {} (detected type: {}, geohash: {}, lat: {}, lon: {})",
          etxTopic, nmiTopic != null ? nmiTopic : "-", detectedMessageType, geohash, latitude,
          longitude);

      Instant timestamp = depositedAt.toInstant(ZoneOffset.UTC);
      handleProcessingSuccess(metricTopics, timestamp.toString(), depositedAt, asn1Hex,
          fanout.mqttBrokerTargets().isEmpty() ? null : fanout.mqttBrokerTargets(),
          detectedMessageType);
    } catch (Exception e) {
      handleProcessingError(e, topicSet, depositedAt.toInstant(ZoneOffset.UTC).toEpochMilli(),
          asn1Hex);
    }
  }
}
