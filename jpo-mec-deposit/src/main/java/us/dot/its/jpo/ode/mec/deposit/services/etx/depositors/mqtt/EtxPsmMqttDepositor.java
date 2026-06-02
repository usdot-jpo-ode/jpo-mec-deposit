package us.dot.its.jpo.ode.mec.deposit.services.etx.depositors.mqtt;

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
import us.dot.its.jpo.asn.j2735.r2024.PersonalSafetyMessage.PersonalSafetyMessage;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.config.condition.ConditionalOnAnyMqttBrokerMqttDepositor;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.BrokerPublishPayload;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttBrokerTarget;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttFanoutPublishResult;
import us.dot.its.jpo.ode.mec.deposit.services.base.AbstractEtxMqttDepositor;
import us.dot.its.jpo.ode.mec.deposit.services.etx.EtxMqttPublishService;
import us.dot.its.jpo.ode.mec.deposit.services.etx.mqtt.EtxBrokerPublisher;
import us.dot.its.jpo.ode.mec.deposit.services.mqtt.MultiBrokerPublishService;
import us.dot.its.jpo.ode.mec.deposit.utils.PositionConversionUtil;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttProtobufBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttTopicBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.NmiMqttTopicBuilder;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;

/**
 * Depositor for PSM (Personal Safety Message) via MQTT to ETX, NMI, and AV brokers.
 */
@Component
@Slf4j
@ConditionalOnAnyMqttBrokerMqttDepositor("psm")
public class EtxPsmMqttDepositor extends AbstractEtxMqttDepositor {
  private final MultiBrokerPublishService multiBrokerPublishService;

  /**
   * Primary Spring constructor.
   */
  @Autowired
  public EtxPsmMqttDepositor(MecDepositProperties mecDepositProperties, EtxProperties etxProperties,
      EtxMqttProperties mqttProperties, @Nullable EtxMqttPublishService mqttService,
      MeterRegistry registry, MultiBrokerPublishService multiBrokerPublishService,
      KafkaTemplate<String, String> kafkaTemplate) {
    super(mecDepositProperties, etxProperties, mqttProperties, EtxMessageType.PSM, mqttService,
        registry, kafkaTemplate);
    this.multiBrokerPublishService = multiBrokerPublishService;
  }

  /**
   * Backward-compatible constructor used by unit tests.
   */
  public EtxPsmMqttDepositor(MecDepositProperties mecDepositProperties, EtxProperties etxProperties,
      EtxMqttProperties mqttProperties, EtxMqttPublishService mqttService, MeterRegistry registry,
      KafkaTemplate<String, String> kafkaTemplate) {
    this(mecDepositProperties, etxProperties, mqttProperties, mqttService, registry,
        new MultiBrokerPublishService(List.of(new EtxBrokerPublisher(mqttService)), mqttProperties,
            registry),
        kafkaTemplate);
  }

  /**
   * Listens for PSM messages from Kafka and deposits them to MQTT topics.
   *
   * @param message PSM ODE frame JSON (Kafka topic {@code topic.OdePsmJson} by default)
   */
  @KafkaListener(topics = "${mec-deposit.etx.mqtt-brokers.etx.depositors.psm.mqtt.kafka-topic}",
      groupId = "${spring.kafka.consumer.group-id}-psm-mqtt-depositor",
      concurrency = "${spring.kafka.listener.concurrency:1}",
      containerFactory = "kafkaListenerContainerFactory")
  public void psmDepositListener(String message) {
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

      PersonalSafetyMessage psm = (PersonalSafetyMessage) msg.getPayload().getData().getValue();
      Position3D position = psm.getPosition();
      if (position == null) {
        log.warn("Skipping PSM with no position");
        return;
      }
      double latitude = PositionConversionUtil.convertRefPointToLat(position);
      double longitude = PositionConversionUtil.convertRefPointToLon(position);
      LocalDateTime depositedAt = LocalDateTime.now(ZoneOffset.UTC);
      Instant depositedInstant = depositedAt.toInstant(ZoneOffset.UTC);
      byte[] etxPayload = EtxMqttProtobufBuilder.toEtxMqttWirePayload(rawMessageBytes,
          mqttProperties.getMessageFormat(), depositedInstant, latitude, longitude);

      topic = EtxMqttTopicBuilder.buildRegionalTopic(messageType, latitude, longitude,
          mqttProperties.getPrecision(), mqttProperties.getVendor(),
          mqttProperties.getMessageFormat(), etxProperties.getClientType(),
          etxProperties.getClientSubType());

      Map<MqttBrokerTarget, BrokerPublishPayload> payloads = new EnumMap<>(MqttBrokerTarget.class);
      if (multiBrokerPublishService.isTargetActive(MqttBrokerTarget.ETX)
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.ETX, "psm")) {
        payloads.put(MqttBrokerTarget.ETX, BrokerPublishPayload.builder()
            .target(MqttBrokerTarget.ETX).topics(Set.of(topic)).payload(etxPayload).build());
      }
      if (multiBrokerPublishService.isTargetActive(MqttBrokerTarget.NMI)
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.NMI, "psm")) {
        String nmiTopic = NmiMqttTopicBuilder.buildTopicFromCoordinates(latitude, longitude,
            etxProperties.mqttTopicPrecision(MqttBrokerTarget.NMI), messageType);
        payloads.put(MqttBrokerTarget.NMI,
            BrokerPublishPayload.builder().target(MqttBrokerTarget.NMI).topics(Set.of(nmiTopic))
                .payload(rawMessageBytes).build());
      }
        if (multiBrokerPublishService.isTargetActive(MqttBrokerTarget.AV)
            && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.AV, "psm")) {
          String avTopic = NmiMqttTopicBuilder.buildTopicFromCoordinates(latitude, longitude,
              etxProperties.mqttTopicPrecision(MqttBrokerTarget.AV), messageType);
          payloads.put(MqttBrokerTarget.AV, BrokerPublishPayload.builder().target(MqttBrokerTarget.AV)
              .topics(Set.of(avTopic)).payload(rawMessageBytes).build());
        }
        if (multiBrokerPublishService.isTargetActive(MqttBrokerTarget.MB)
            && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.MB, "psm")) {
          String mbTopic = NmiMqttTopicBuilder.buildTopicFromCoordinates(latitude, longitude,
              etxProperties.mqttTopicPrecision(MqttBrokerTarget.MB), messageType);
          payloads.put(MqttBrokerTarget.MB, BrokerPublishPayload.builder().target(MqttBrokerTarget.MB)
              .topics(Set.of(mbTopic)).payload(rawMessageBytes).build());
        }
        MqttFanoutPublishResult fanout = multiBrokerPublishService.publish(payloads, retain);
      if (fanout.publishedTopics().isEmpty()) {
        log.warn("PSM MQTT fanout completed with zero publishes (targets in map: {})",
            payloads.keySet());
      } else {
        log.debug("Successfully sent PSM message to MQTT topic: {}", topic);
      }

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
