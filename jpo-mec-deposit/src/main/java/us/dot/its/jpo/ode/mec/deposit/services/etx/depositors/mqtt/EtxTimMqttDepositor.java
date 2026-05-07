package us.dot.its.jpo.ode.mec.deposit.services.etx.depositors.mqtt;

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
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.GeoRoutedMsg;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttBrokerTarget;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttFanoutPublishResult;
import us.dot.its.jpo.ode.mec.deposit.services.base.AbstractEtxMqttDepositor;
import us.dot.its.jpo.ode.mec.deposit.services.etx.EtxMqttPublishService;
import us.dot.its.jpo.ode.mec.deposit.services.etx.mqtt.EtxBrokerPublisher;
import us.dot.its.jpo.ode.mec.deposit.services.mqtt.MultiBrokerPublishService;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttProtobufBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttTopicBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.NmiMqttTopicBuilder;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;
import us.dot.its.jpo.asn.j2735.r2024.TravelerInformation.TravelerInformation;

/**
 * Depositor class for handling TIM messages via MQTT integration with ETX.
 */
@Component
@Slf4j
@ConditionalOnAnyMqttBrokerMqttDepositor("tim")
public class EtxTimMqttDepositor extends AbstractEtxMqttDepositor {
  private final MultiBrokerPublishService multiBrokerPublishService;

  @Autowired
  public EtxTimMqttDepositor(MecDepositProperties mecDepositProperties, EtxProperties etxProperties,
      EtxMqttProperties mqttProperties, @Nullable EtxMqttPublishService mqttService,
      MeterRegistry registry,
      MultiBrokerPublishService multiBrokerPublishService,
      KafkaTemplate<String, String> kafkaTemplate) {
    super(mecDepositProperties, etxProperties, mqttProperties, EtxMessageType.TIM, mqttService,
        registry, kafkaTemplate);
    this.multiBrokerPublishService = multiBrokerPublishService;
  }

  /**
   * Backward-compatible constructor used by unit tests.
   */
  public EtxTimMqttDepositor(MecDepositProperties mecDepositProperties, EtxProperties etxProperties,
      EtxMqttProperties mqttProperties, EtxMqttPublishService mqttService, MeterRegistry registry,
      KafkaTemplate<String, String> kafkaTemplate) {
    this(mecDepositProperties, etxProperties, mqttProperties, mqttService, registry,
        new MultiBrokerPublishService(List.of(new EtxBrokerPublisher(mqttService)), mqttProperties,
            registry),
        kafkaTemplate);
  }

  /**
   * Listens for TIM messages from Kafka and deposits them to appropriate MQTT topics.
   *
   * @param message The TIM message from Kafka in JSON format
   */
  @KafkaListener(topics = "${mec-deposit.etx.mqtt-brokers.etx.depositors.tim.mqtt.kafka-topic}",
      groupId = "${spring.kafka.consumer.group-id}-tim-mqtt-depositor",
      concurrency = "${spring.kafka.listener.concurrency:1}",
      containerFactory = "kafkaListenerContainerFactory")
  public void timDepositListener(String message) {
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

      byte[] messageBytes = Hex.decode(msg.getMetadata().getAsn1());
      var timMsg = (TravelerInformation) msg.getPayload().getData().getValue();
      LocalDateTime depositedAt = LocalDateTime.now(ZoneOffset.UTC);

      // If the message format is J2735_GR, we need to convert the message to a GeoRoutedMsg
      if (mqttProperties.getMessageFormat() == EtxMqttMessageFormat.J2735_GR) {
        Instant timestamp = depositedAt.toInstant(ZoneOffset.UTC);
        GeoRoutedMsg geoRoutedMsg =
            EtxMqttProtobufBuilder.buildGeoRoutedMsg(messageBytes, timestamp);
        messageBytes = geoRoutedMsg.toByteArray();
      }

      var dataFramesList = timMsg.getDataFrames();
      topicSet = EtxMqttTopicBuilder.getTimTopicList(dataFramesList, mqttProperties.getVendor(),
          mqttProperties.getPrecision(), mqttProperties.getMessageFormat(),
          etxProperties.getClientType(), etxProperties.getClientSubType());

      Map<MqttBrokerTarget, BrokerPublishPayload> payloads = new EnumMap<>(MqttBrokerTarget.class);
      if (multiBrokerPublishService.isTargetActive(MqttBrokerTarget.ETX)
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.ETX, "tim")) {
        payloads.put(MqttBrokerTarget.ETX, BrokerPublishPayload.builder().target(MqttBrokerTarget.ETX)
            .topics(topicSet).payload(messageBytes).build());
      }
      if (multiBrokerPublishService.isTargetActive(MqttBrokerTarget.NMI)
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.NMI, "tim")) {
        Set<String> nmiTopicSet = NmiMqttTopicBuilder.getTimTopicList(dataFramesList,
            etxProperties.mqttTopicPrecision(MqttBrokerTarget.NMI), messageType);
        payloads.put(MqttBrokerTarget.NMI, BrokerPublishPayload.builder().target(MqttBrokerTarget.NMI)
            .topics(nmiTopicSet).payload(messageBytes).build());
      }
      if (multiBrokerPublishService.isTargetActive(MqttBrokerTarget.AV)
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.AV, "tim")) {
        Set<String> avTopicSet = NmiMqttTopicBuilder.getTimTopicList(dataFramesList,
            etxProperties.mqttTopicPrecision(MqttBrokerTarget.AV), messageType);
        payloads.put(MqttBrokerTarget.AV, BrokerPublishPayload.builder().target(MqttBrokerTarget.AV)
            .topics(avTopicSet).payload(messageBytes).build());
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

