package us.dot.its.jpo.ode.mec.deposit.services.depositors.mqtt;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.config.condition.ConditionalOnAnyMqttBrokerMqttDepositor;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.BrokerPublishPayload;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttBrokerTarget;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttFanoutPublishResult;
import us.dot.its.jpo.ode.mec.deposit.services.base.AbstractMqttDepositor;
import us.dot.its.jpo.ode.mec.deposit.services.etx.EtxMqttPublishService;
import us.dot.its.jpo.ode.mec.deposit.services.etx.mqtt.EtxBrokerPublisher;
import us.dot.its.jpo.ode.mec.deposit.services.mqtt.MultiBrokerPublishService;
import us.dot.its.jpo.ode.mec.deposit.utils.PositionConversionUtil;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttProtobufBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttTopicBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.NmiMqttTopicBuilder;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;
import us.dot.its.jpo.asn.j2735.r2024.BasicSafetyMessage.BasicSafetyMessage;

/**
 * Depositor for BSM (Basic Safety Message) to ETX, NMI, and AV MQTT brokers.
 */
@Component
@Slf4j
@ConditionalOnAnyMqttBrokerMqttDepositor("bsm")
public class BsmMqttDepositor extends AbstractMqttDepositor {
  private final MultiBrokerPublishService multiBrokerPublishService;

  @Autowired
  public BsmMqttDepositor(MecDepositProperties mecDepositProperties, EtxProperties etxProperties,
      EtxMqttProperties mqttProperties, @Nullable EtxMqttPublishService mqttService,
      MeterRegistry registry,
      MultiBrokerPublishService multiBrokerPublishService,
      KafkaTemplate<String, String> kafkaTemplate) {
    super(mecDepositProperties, etxProperties, mqttProperties, EtxMessageType.BSM, mqttService,
        registry, kafkaTemplate);
    this.multiBrokerPublishService = multiBrokerPublishService;
  }

  /**
   * Backward-compatible constructor used by unit tests.
   */
  public BsmMqttDepositor(MecDepositProperties mecDepositProperties, EtxProperties etxProperties,
      EtxMqttProperties mqttProperties, EtxMqttPublishService mqttService, MeterRegistry registry,
      KafkaTemplate<String, String> kafkaTemplate) {
    this(mecDepositProperties, etxProperties, mqttProperties, mqttService, registry,
        new MultiBrokerPublishService(List.of(new EtxBrokerPublisher(mqttService)), mqttProperties,
            registry),
        kafkaTemplate);
  }

  /**
   * Listens for BSM messages from Kafka and deposits them to appropriate MQTT topics.
   *
   * @param message The BSM message from Kafka in JSON format
   */
  @KafkaListener(topics = "${mec-deposit.etx.mqtt-brokers.etx.depositors.bsm.mqtt.kafka-topic}",
      groupId = "${spring.kafka.consumer.group-id}-bsm-mqtt-depositor",
      concurrency = "${spring.kafka.listener.concurrency:1}",
      containerFactory = "kafkaListenerContainerFactory")
  public void bsmDepositListener(String message) {
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

      BasicSafetyMessage bsm = (BasicSafetyMessage) msg.getPayload().getData().getValue();
      double latitude = PositionConversionUtil.convertJ2735LatToLat(bsm.getCoreData().getLat());
      double longitude = PositionConversionUtil.convertJ2735LonToLon(bsm.getCoreData().getLong_());
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
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.ETX, "bsm")) {
        payloads.put(MqttBrokerTarget.ETX, BrokerPublishPayload.builder().target(MqttBrokerTarget.ETX)
            .topics(Set.of(topic)).payload(etxPayload).build());
      }
      if (multiBrokerPublishService.isTargetActive(MqttBrokerTarget.NMI)
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.NMI, "bsm")) {
        String nmiTopic = NmiMqttTopicBuilder.buildTopicFromCoordinates(latitude, longitude,
            etxProperties.mqttTopicPrecision(MqttBrokerTarget.NMI), messageType);
        payloads.put(MqttBrokerTarget.NMI, BrokerPublishPayload.builder().target(MqttBrokerTarget.NMI)
            .topics(Set.of(nmiTopic)).payload(rawMessageBytes).build());
      }
      if (multiBrokerPublishService.isTargetActive(MqttBrokerTarget.AV)
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.AV, "bsm")) {
        String avTopic = NmiMqttTopicBuilder.buildTopicFromCoordinates(latitude, longitude,
            etxProperties.mqttTopicPrecision(MqttBrokerTarget.AV), messageType);
        payloads.put(MqttBrokerTarget.AV, BrokerPublishPayload.builder().target(MqttBrokerTarget.AV)
            .topics(Set.of(avTopic)).payload(rawMessageBytes).build());
      }
      MqttFanoutPublishResult fanout = multiBrokerPublishService.publish(payloads, retain);
      log.debug("Successfully sent BSM message to MQTT topic: {}", topic);

      Set<String> metricTopics = fanout.publishedTopics().isEmpty()
          ? MultiBrokerPublishService.unionPayloadTopics(payloads) : fanout.publishedTopics();
      handleProcessingSuccess(metricTopics, odeReceivedAt, depositedAt, asn1Hex,
          fanout.mqttBrokerTargets().isEmpty() ? null : fanout.mqttBrokerTargets());
    } catch (Exception e) {
      handleProcessingError(e, Set.of(topic),
          odeReceivedAt != null ? Instant.parse(odeReceivedAt).toEpochMilli() : 0, asn1Hex);
    }
  }
}
