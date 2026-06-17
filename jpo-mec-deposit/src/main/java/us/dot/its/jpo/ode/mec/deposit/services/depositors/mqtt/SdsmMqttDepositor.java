package us.dot.its.jpo.ode.mec.deposit.services.depositors.mqtt;

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
import us.dot.its.jpo.asn.j2735.r2024.Common.Position3D;
import us.dot.its.jpo.asn.j2735.r2024.SensorDataSharingMessage.SensorDataSharingMessage;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.config.condition.ConditionalOnAnyMqttBrokerMqttDepositor;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.BrokerPublishPayload;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttBrokerTarget;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttFanoutPublishResult;
import us.dot.its.jpo.ode.mec.deposit.services.base.AbstractMqttDepositor;
import us.dot.its.jpo.ode.mec.deposit.services.etx.mqtt.EtxBrokerPublisher;
import us.dot.its.jpo.ode.mec.deposit.services.etx.mqtt.EtxMqttPublishService;
import us.dot.its.jpo.ode.mec.deposit.services.mqtt.MultiBrokerPublishService;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttProtobufBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttTopicBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.NmiMqttTopicBuilder;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;

/**
 * Depositor for handling SDSM messages via multi-broker MQTT fanout.
 */
@Component
@Slf4j
@ConditionalOnAnyMqttBrokerMqttDepositor("sdsm")
public class SdsmMqttDepositor extends AbstractMqttDepositor {

  private static final double MICRODEGREES_TO_DECIMAL_DEGREES_CONVERSION_FACTOR = 1.0 / 10000000.0;
  private final MultiBrokerPublishService multiBrokerPublishService;

  /**
   * Primary Spring constructor.
   */
  @Autowired
  public SdsmMqttDepositor(MecDepositProperties mecDepositProperties, EtxProperties etxProperties,
      EtxMqttProperties mqttProperties, @Nullable EtxMqttPublishService mqttService,
      MeterRegistry registry, MultiBrokerPublishService multiBrokerPublishService,
      KafkaTemplate<String, String> kafkaTemplate) {
    super(mecDepositProperties, etxProperties, mqttProperties, EtxMessageType.SDSM, mqttService,
        registry, kafkaTemplate);
    this.multiBrokerPublishService = multiBrokerPublishService;
  }

  /**
   * Backward-compatible constructor used by unit tests.
   */
  public SdsmMqttDepositor(MecDepositProperties mecDepositProperties, EtxProperties etxProperties,
      EtxMqttProperties mqttProperties, EtxMqttPublishService mqttService, MeterRegistry registry,
      KafkaTemplate<String, String> kafkaTemplate) {
    this(mecDepositProperties, etxProperties, mqttProperties, mqttService, registry,
        new MultiBrokerPublishService(List.of(new EtxBrokerPublisher(mqttService)),
            Set.of(MqttBrokerTarget.ETX), registry),
        kafkaTemplate);
  }

  /**
   * Listens for SDSM messages from Kafka and deposits them to appropriate MQTT topics.
   *
   * @param message The SDSM message from Kafka in JSON format
   */
  @KafkaListener(topics = "${mec-deposit.etx.mqtt-brokers.etx.depositors.sdsm.mqtt.kafka-topic}",
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

      byte[] rawMessageBytes = Hex.decode(msg.getMetadata().getAsn1());

      SensorDataSharingMessage sdsm =
          (SensorDataSharingMessage) msg.getPayload().getData().getValue();
      Position3D refPoint = sdsm.getRefPos();
      LocalDateTime depositedAt = LocalDateTime.now(ZoneOffset.UTC);

      Double latitude =
          (double) refPoint.getLat().getValue() * MICRODEGREES_TO_DECIMAL_DEGREES_CONVERSION_FACTOR;
      Double longitude = (double) refPoint.getLong_().getValue()
          * MICRODEGREES_TO_DECIMAL_DEGREES_CONVERSION_FACTOR;

      Instant depositedInstant = depositedAt.toInstant(ZoneOffset.UTC);
      byte[] etxPayload = EtxMqttProtobufBuilder.toEtxMqttWirePayload(rawMessageBytes,
          mqttProperties.getMessageFormat(), depositedInstant, latitude, longitude);

      topic = EtxMqttTopicBuilder.buildRegionalTopic(messageType, latitude, longitude,
          mqttProperties.getPrecision(), mqttProperties.getVendor(),
          mqttProperties.getMessageFormat(), etxProperties.getClientType(),
          etxProperties.getClientSubType());

      Map<MqttBrokerTarget, BrokerPublishPayload> payloads = new EnumMap<>(MqttBrokerTarget.class);
      if (multiBrokerPublishService.isTargetActive(MqttBrokerTarget.ETX)
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.ETX, "sdsm")) {
        payloads.put(MqttBrokerTarget.ETX, BrokerPublishPayload.builder()
            .target(MqttBrokerTarget.ETX).topics(Set.of(topic)).payload(etxPayload).build());
      }
      if (multiBrokerPublishService.isTargetActive(MqttBrokerTarget.NMI)
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.NMI, "sdsm")) {
        String nmiTopic = NmiMqttTopicBuilder.buildTopicFromCoordinates(latitude, longitude,
            etxProperties.mqttTopicPrecision(MqttBrokerTarget.NMI), messageType);
        payloads.put(MqttBrokerTarget.NMI,
            BrokerPublishPayload.builder().target(MqttBrokerTarget.NMI).topics(Set.of(nmiTopic))
                .payload(rawMessageBytes).build());
      }
      if (multiBrokerPublishService.isTargetActive(MqttBrokerTarget.AV)
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.AV, "sdsm")) {
        String avTopic = NmiMqttTopicBuilder.buildTopicFromCoordinates(latitude, longitude,
            etxProperties.mqttTopicPrecision(MqttBrokerTarget.AV), messageType);
        payloads.put(MqttBrokerTarget.AV, BrokerPublishPayload.builder().target(MqttBrokerTarget.AV)
            .topics(Set.of(avTopic)).payload(rawMessageBytes).build());
      }
      if (multiBrokerPublishService.isTargetActive(MqttBrokerTarget.MB)
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.MB, "sdsm")) {
        String mbTopic = NmiMqttTopicBuilder.buildTopicFromCoordinates(latitude, longitude,
            etxProperties.mqttTopicPrecision(MqttBrokerTarget.MB), messageType);
        payloads.put(MqttBrokerTarget.MB, BrokerPublishPayload.builder().target(MqttBrokerTarget.MB)
            .topics(Set.of(mbTopic)).payload(rawMessageBytes).build());
      }
      MqttFanoutPublishResult fanout = multiBrokerPublishService.publish(payloads, retain);
      log.debug("Successfully sent SDSM message to MQTT topic: {}", topic);

      Set<String> metricTopics = fanout.publishedTopics().isEmpty()
          ? MultiBrokerPublishService.unionPayloadTopics(payloads)
          : fanout.publishedTopics();
      handleProcessingSuccess(metricTopics, odeReceivedAt, depositedAt, asn1Hex,
          fanout.mqttBrokerTargets().isEmpty() ? null : fanout.mqttBrokerTargets());
    } catch (Exception e) {
      handleProcessingError(e, Set.of(topic),
          odeReceivedAt != null ? Instant.parse(odeReceivedAt).toEpochMilli() : 0, asn1Hex);
    }
  }
}
