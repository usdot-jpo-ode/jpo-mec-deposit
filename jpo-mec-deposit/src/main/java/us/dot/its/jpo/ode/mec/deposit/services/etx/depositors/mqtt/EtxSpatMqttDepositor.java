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
import org.springframework.lang.Nullable;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.config.condition.ConditionalOnAnyMqttBrokerMqttDepositor;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
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
import us.dot.its.jpo.ode.mec.deposit.utils.MapRefPointCollector;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttProtobufBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttTopicBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.NmiMqttTopicBuilder;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;
import us.dot.its.jpo.asn.j2735.r2024.SPAT.SPAT;

/**
 * Depositor class for handling SPAT messages via MQTT integration with ETX.
 */
@Component
@Slf4j
@ConditionalOnAnyMqttBrokerMqttDepositor("spat")
public class EtxSpatMqttDepositor extends AbstractEtxMqttDepositor {
  private final MultiBrokerPublishService multiBrokerPublishService;

  @Autowired
  private MapRefPointCollector mapDataCollector;

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
  @Autowired
  public EtxSpatMqttDepositor(MecDepositProperties mecDepositProperties,
      EtxProperties etxProperties, EtxMqttProperties mqttProperties,
      @Nullable EtxMqttPublishService mqttService, MeterRegistry registry,
      MultiBrokerPublishService multiBrokerPublishService,
      KafkaTemplate<String, String> kafkaTemplate) {
    super(mecDepositProperties, etxProperties, mqttProperties, EtxMessageType.SPAT, mqttService,
        registry, kafkaTemplate);
    this.multiBrokerPublishService = multiBrokerPublishService;
  }

  /**
   * Backward-compatible constructor used by unit tests.
   */
  public EtxSpatMqttDepositor(MecDepositProperties mecDepositProperties,
      EtxProperties etxProperties, EtxMqttProperties mqttProperties, EtxMqttPublishService mqttService,
      MeterRegistry registry, KafkaTemplate<String, String> kafkaTemplate) {
    this(mecDepositProperties, etxProperties, mqttProperties, mqttService, registry,
        new MultiBrokerPublishService(List.of(new EtxBrokerPublisher(mqttService)), mqttProperties,
            registry),
        kafkaTemplate);
  }

  /**
   * Listens for SPAT messages from Kafka and deposits them to appropriate MQTT topics.
   *
   * @param message The SPAT message from Kafka in JSON format
   */
  @KafkaListener(topics = "${mec-deposit.etx.mqtt-brokers.etx.depositors.spat.mqtt.kafka-topic}",
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

      boolean wantEtx = multiBrokerPublishService.isTargetActive(MqttBrokerTarget.ETX)
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.ETX, "spat")
          && etxProperties.passesSpatIntersectionFilter(MqttBrokerTarget.ETX, spatMsg);
      boolean wantNmi = multiBrokerPublishService.isTargetActive(MqttBrokerTarget.NMI)
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.NMI, "spat")
          && etxProperties.passesSpatIntersectionFilter(MqttBrokerTarget.NMI, spatMsg);
      boolean wantAv = multiBrokerPublishService.isTargetActive(MqttBrokerTarget.AV)
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.AV, "spat")
          && etxProperties.passesSpatIntersectionFilter(MqttBrokerTarget.AV, spatMsg);
      if (!wantEtx && !wantNmi && !wantAv) {
        return;
      }

      byte[] rawMessageBytes = Hex.decode(msg.getMetadata().getAsn1());
      LocalDateTime depositedAt = LocalDateTime.now(ZoneOffset.UTC);
      Instant depositedInstant = depositedAt.toInstant(ZoneOffset.UTC);
      byte[] etxPayload = EtxMqttProtobufBuilder.toEtxMqttWirePayload(rawMessageBytes,
          mqttProperties.getMessageFormat(), depositedInstant, null, null);

      topicSet = EtxMqttTopicBuilder.getSpatTopicList(spatMsg, mapDataCollector,
          mqttProperties.getVendor(), mqttProperties.getPrecision(),
          mqttProperties.getMessageFormat(), etxProperties.getClientType(),
          etxProperties.getClientSubType());
      Map<MqttBrokerTarget, BrokerPublishPayload> payloads = new EnumMap<>(MqttBrokerTarget.class);
      if (wantEtx) {
        payloads.put(MqttBrokerTarget.ETX, BrokerPublishPayload.builder().target(MqttBrokerTarget.ETX)
            .topics(topicSet).payload(etxPayload).build());
      }
      if (wantNmi) {
        Set<String> nmiTopicSet = NmiMqttTopicBuilder.getSpatTopicList(spatMsg, mapDataCollector,
            etxProperties.mqttTopicPrecision(MqttBrokerTarget.NMI), messageType);
        payloads.put(MqttBrokerTarget.NMI, BrokerPublishPayload.builder().target(MqttBrokerTarget.NMI)
            .topics(nmiTopicSet).payload(rawMessageBytes).build());
      }
      if (wantAv) {
        Set<String> avTopicSet = NmiMqttTopicBuilder.getSpatTopicList(spatMsg, mapDataCollector,
            etxProperties.mqttTopicPrecision(MqttBrokerTarget.AV), messageType);
        payloads.put(MqttBrokerTarget.AV, BrokerPublishPayload.builder().target(MqttBrokerTarget.AV)
            .topics(avTopicSet).payload(rawMessageBytes).build());
      }
      MqttFanoutPublishResult fanout = multiBrokerPublishService.publish(payloads, retain);
      Set<String> metricTopics = fanout.publishedTopics().isEmpty()
          ? MultiBrokerPublishService.unionPayloadTopics(payloads) : fanout.publishedTopics();
      handleProcessingSuccess(metricTopics, odeReceivedAt, depositedAt, asn1Hex,
          fanout.mqttBrokerTargets().isEmpty() ? null : fanout.mqttBrokerTargets());
    } catch (Exception e) {
      handleProcessingError(e, topicSet,
          odeReceivedAt != null ? Instant.parse(odeReceivedAt).toEpochMilli() : 0, asn1Hex);
    }
  }
}
