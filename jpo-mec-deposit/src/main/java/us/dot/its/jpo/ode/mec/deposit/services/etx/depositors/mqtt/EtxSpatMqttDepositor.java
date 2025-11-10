package us.dot.its.jpo.ode.mec.deposit.services.etx.depositors.mqtt;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Autowired;
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
import us.dot.its.jpo.ode.mec.deposit.utils.MapRefPointCollector;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttProtobufBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttTopicBuilder;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;
import us.dot.its.jpo.asn.j2735.r2024.SPAT.SPAT;

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
  private Boolean intersectionFilterEnabled;
  private List<Long> allowedIntersectionIds;
  private List<Long> blockedIntersectionIds;

  /**
   * Constructs a new EtxSpatMqttDepositor with the specified dependencies.
   *
   * @param mecDepositProperties Core MEC deposit configuration properties
   * @param etxProperties ETX-specific configuration properties
   * @param mqttProperties MQTT-specific configuration properties for ETX
   * @param mqttService Service for publishing messages to MQTT
   * @param registry Metrics registry for monitoring and instrumentation
   * @param kafkaTemplate Template for Kafka operations
   */
  public EtxSpatMqttDepositor(MecDepositProperties mecDepositProperties,
      EtxProperties etxProperties, EtxMqttProperties mqttProperties,
      EtxMqttPublishService mqttService, MeterRegistry registry,
      KafkaTemplate<String, String> kafkaTemplate) {
    super(mecDepositProperties, etxProperties, mqttProperties, EtxMessageType.SPAT, mqttService,
        registry, kafkaTemplate);
    this.intersectionFilterEnabled =
        etxProperties.getDepositors().getSpat().getMqtt().getIntersectionFilter().getEnabled();
    this.allowedIntersectionIds = etxProperties.getDepositors().getSpat().getMqtt()
        .getIntersectionFilter().getAllowedIntersectionIds();
    this.blockedIntersectionIds = etxProperties.getDepositors().getSpat().getMqtt()
        .getIntersectionFilter().getBlockedIntersectionIds();
  }

  private boolean shouldProcessIntersection(SPAT spatMsg) {
    if (!intersectionFilterEnabled) {
      return true;
    }

    // Get intersection ID from the first intersection in the SPAT message
    if (spatMsg.getIntersections() != null && spatMsg.getIntersections().size() > 0) {
      Long intersectionId = spatMsg.getIntersections().get(0).getId().getId().getValue();

      // Check if intersection is blocked
      if (blockedIntersectionIds != null && blockedIntersectionIds.contains(intersectionId)) {
        log.debug("Filtering out SPAT message for blocked intersection ID: {}", intersectionId);
        return false;
      }

      // If allowlist is empty, allow all non-blocked intersections
      if (allowedIntersectionIds == null || allowedIntersectionIds.isEmpty()) {
        return true;
      }

      // Check if intersection is explicitly allowed
      boolean allowed = allowedIntersectionIds.contains(intersectionId);
      if (!allowed) {
        log.debug("Filtering out SPAT message for non-allowed intersection ID: {}", intersectionId);
      }
      return allowed;
    }
    return false;
  }

  /**
   * Listens for SPAT messages from Kafka and deposits them to appropriate MQTT topics.
   *
   * @param message The SPAT message from Kafka in JSON format
   */
  @Async("kafkaListenerExecutor")
  @KafkaListener(topics = "${mec-deposit.etx.depositors.spat.mqtt.kafka-topic}",
      groupId = "${spring.kafka.consumer.group-id}-spat-mqtt-depositor",
      concurrency = "${spring.kafka.listener.concurrency:1}",
      containerFactory = "kafkaListenerContainerFactory")
  public void spatDepositListener(String message) {
    boolean retain = false;
    Set<String> topicSet = null;
    String odeReceivedAt = null;
    String asn1Hex = "";
    try {
      OdeMessageFrameData msg = mapper.readValue(message, OdeMessageFrameData.class);
      odeReceivedAt = msg.getMetadata().getOdeReceivedAt();
      asn1Hex = msg.getMetadata().getAsn1();

      if (isMessageStale(odeReceivedAt)) {
        return;
      }

      SPAT spatMsg = (SPAT) msg.getPayload().getData().getValue();

      // Add intersection filtering check
      if (!shouldProcessIntersection(spatMsg)) {
        return;
      }

      byte[] messageBytes = Hex.decode(msg.getMetadata().getAsn1());
      LocalDateTime depositedAt = LocalDateTime.now(ZoneOffset.UTC);

      // If the message format is J2735_GR, we need to convert the message to a GeoRoutedMsg
      if (mqttProperties.getMessageFormat() == EtxMqttMessageFormat.J2735_GR) {
        Instant timestamp = depositedAt.toInstant(ZoneOffset.UTC);
        GeoRoutedMsg geoRoutedMsg =
            EtxMqttProtobufBuilder.buildGeoRoutedMsg(messageBytes, timestamp);
        messageBytes = geoRoutedMsg.toByteArray();
      }

      topicSet = EtxMqttTopicBuilder.getSpatTopicList(spatMsg, mapDataCollector,
          mqttProperties.getVendor(), mqttProperties.getPrecision(),
          mqttProperties.getMessageFormat(), etxProperties.getClientType(),
          etxProperties.getClientSubType());

      for (String topic : topicSet) {
        mqttService.publishAsn1Bytes(topic, messageBytes, retain);
        log.debug("Successfully sent SPaT message to MQTT topic: {}", topic);
      }

      handleProcessingSuccess(topicSet, odeReceivedAt, depositedAt, asn1Hex);
    } catch (Exception e) {
      handleProcessingError(e, topicSet,
          odeReceivedAt != null ? Instant.parse(odeReceivedAt).toEpochMilli() : 0, asn1Hex);
    }
  }
}
