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
import us.dot.its.jpo.asn.j2735.r2024.TravelerInformation.TravelerDataFrameList;
import us.dot.its.jpo.asn.j2735.r2024.TravelerInformation.TravelerInformation;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.config.condition.ConditionalOnAnyMqttBrokerMqttDepositor;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.PartnerClient;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.PartnerTokenManager;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.BrokerPublishPayload;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttBrokerTarget;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttFanoutPublishResult;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.GeofencePreviewResponse;
import us.dot.its.jpo.ode.mec.deposit.services.base.AbstractMqttDepositor;
import us.dot.its.jpo.ode.mec.deposit.services.etx.mqtt.EtxBrokerPublisher;
import us.dot.its.jpo.ode.mec.deposit.services.etx.mqtt.EtxMqttPublishService;
import us.dot.its.jpo.ode.mec.deposit.services.mqtt.MultiBrokerPublishService;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttProtobufBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttTopicBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.NmiMqttTopicBuilder;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;

/**
 * Depositor for handling TIM messages via multi-broker MQTT fanout.
 */
@Component
@Slf4j
@ConditionalOnAnyMqttBrokerMqttDepositor("tim")
public class TimMqttDepositor extends AbstractMqttDepositor {
  private final MultiBrokerPublishService multiBrokerPublishService;
  @Nullable
  private final PartnerClient partnerClient;
  @Nullable
  private final PartnerTokenManager tokenManager;

  /**
   * Primary Spring constructor.
   */
  @Autowired
  public TimMqttDepositor(MecDepositProperties mecDepositProperties, EtxProperties etxProperties,
      EtxMqttProperties mqttProperties, @Nullable EtxMqttPublishService mqttService,
      MeterRegistry registry, MultiBrokerPublishService multiBrokerPublishService,
      KafkaTemplate<String, String> kafkaTemplate, @Nullable PartnerClient partnerClient,
      @Nullable PartnerTokenManager tokenManager) {
    super(mecDepositProperties, etxProperties, mqttProperties, EtxMessageType.TIM, mqttService,
        registry, kafkaTemplate);
    this.multiBrokerPublishService = multiBrokerPublishService;
    this.partnerClient = partnerClient;
    this.tokenManager = tokenManager;
  }

  /**
   * Backward-compatible constructor used by unit tests.
   */
  public TimMqttDepositor(MecDepositProperties mecDepositProperties, EtxProperties etxProperties,
      EtxMqttProperties mqttProperties, EtxMqttPublishService mqttService, MeterRegistry registry,
      KafkaTemplate<String, String> kafkaTemplate) {
    this(mecDepositProperties, etxProperties, mqttProperties, mqttService, registry,
        new MultiBrokerPublishService(List.of(new EtxBrokerPublisher(mqttService)),
            Set.of(MqttBrokerTarget.ETX), registry),
        kafkaTemplate, null, null);
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

      byte[] rawMessageBytes = Hex.decode(msg.getMetadata().getAsn1());
      var timMsg = (TravelerInformation) msg.getPayload().getData().getValue();
      LocalDateTime depositedAt = LocalDateTime.now(ZoneOffset.UTC);
      Instant depositedInstant = depositedAt.toInstant(ZoneOffset.UTC);
      byte[] etxPayload = EtxMqttProtobufBuilder.toEtxMqttWirePayload(rawMessageBytes,
          mqttProperties.getMessageFormat(), depositedInstant, null, null);

      var dataFramesList = timMsg.getDataFrames();
      boolean etxPublish = shouldPublishTo(MqttBrokerTarget.ETX);
      boolean nmiPublish = shouldPublishTo(MqttBrokerTarget.NMI);
      boolean avPublish = shouldPublishTo(MqttBrokerTarget.AV);
      boolean mbPublish = shouldPublishTo(MqttBrokerTarget.MB);
      TimTopicSets topicSets = resolveTimTopicSets(dataFramesList, asn1Hex, etxPublish, nmiPublish,
          avPublish, mbPublish);

      Map<MqttBrokerTarget, BrokerPublishPayload> payloads = new EnumMap<>(MqttBrokerTarget.class);
      if (etxPublish) {
        payloads.put(MqttBrokerTarget.ETX,
            BrokerPublishPayload.builder().target(MqttBrokerTarget.ETX)
                .topics(topicSets.etxTopics()).payload(etxPayload).build());
      }
      if (nmiPublish) {
        payloads.put(MqttBrokerTarget.NMI,
            BrokerPublishPayload.builder().target(MqttBrokerTarget.NMI)
                .topics(topicSets.nmiTopics()).payload(rawMessageBytes).build());
      }
      if (avPublish) {
        payloads.put(MqttBrokerTarget.AV, BrokerPublishPayload.builder().target(MqttBrokerTarget.AV)
            .topics(topicSets.avTopics()).payload(rawMessageBytes).build());
      }
      if (mbPublish) {
        payloads.put(MqttBrokerTarget.MB, BrokerPublishPayload.builder().target(MqttBrokerTarget.MB)
            .topics(topicSets.mbTopics()).payload(rawMessageBytes).build());
      }
      topicSet = topicSets.etxTopics();
      MqttFanoutPublishResult fanout = multiBrokerPublishService.publish(payloads, retain);
      Set<String> metricTopics = fanout.publishedTopics().isEmpty()
          ? MultiBrokerPublishService.unionPayloadTopics(payloads)
          : fanout.publishedTopics();
      handleProcessingSuccess(metricTopics, odeReceivedAt, depositedAt, asn1Hex,
          fanout.mqttBrokerTargets().isEmpty() ? null : fanout.mqttBrokerTargets());
    } catch (Exception e) {
      handleProcessingError(e, topicSet,
          odeReceivedAt != null ? Instant.parse(odeReceivedAt).toEpochMilli() : 0, asn1Hex);
    }
  }

  private boolean shouldPublishTo(MqttBrokerTarget target) {
    return multiBrokerPublishService.isTargetActive(target)
        && etxProperties.isMqttDepositorEnabled(target, "tim");
  }

  private TimTopicSets resolveTimTopicSets(TravelerDataFrameList dataFramesList, String asn1Hex,
      boolean etxPublish, boolean nmiPublish, boolean avPublish, boolean mbPublish) {
    if (etxProperties.isTimMqttGeofencePreviewEnabled()) {
      List<String> geohashes = fetchGeofencePreviewGeohashes(asn1Hex);
      if (!geohashes.isEmpty()) {
        log.debug("Using {} geofence preview geohashes for TIM MQTT topics", geohashes.size());
        return buildTopicSetsFromGeohashes(geohashes, etxPublish, nmiPublish, avPublish, mbPublish);
      }
      log.warn(
          "TIM geofence preview enabled but no geohashes returned; falling back to data-frame regions");
    }
    return buildTopicSetsFromDataFrames(dataFramesList, etxPublish, nmiPublish, avPublish,
        mbPublish);
  }

  private List<String> fetchGeofencePreviewGeohashes(String asn1Hex) {
    if (partnerClient == null || tokenManager == null) {
      log.warn("TIM geofence preview enabled but Partner API client/token manager unavailable");
      return List.of();
    }
    try {
      String token = tokenManager.getValidToken();
      GeofencePreviewResponse preview = partnerClient.previewGeofence(token, asn1Hex);
      if (preview == null || preview.getGeohashes() == null || preview.getGeohashes().isEmpty()) {
        return List.of();
      }
      return preview.getGeohashes();
    } catch (Exception e) {
      log.warn("TIM geofence preview request failed; falling back to data-frame regions", e);
      return List.of();
    }
  }

  private TimTopicSets buildTopicSetsFromGeohashes(List<String> geohashes, boolean etxPublish,
      boolean nmiPublish, boolean avPublish, boolean mbPublish) {
    Set<String> etxTopics = etxPublish
        ? EtxMqttTopicBuilder.getTimTopicListFromGeohashes(geohashes, mqttProperties.getVendor(),
            mqttProperties.getPrecision(), mqttProperties.getMessageFormat(),
            etxProperties.getClientType(), etxProperties.getClientSubType())
        : Set.of();
    Set<String> nmiTopics = nmiPublish ? NmiMqttTopicBuilder.getTimTopicListFromGeohashes(geohashes,
        etxProperties.mqttTopicPrecision(MqttBrokerTarget.NMI), messageType) : Set.of();
    Set<String> avTopics = avPublish ? NmiMqttTopicBuilder.getTimTopicListFromGeohashes(geohashes,
        etxProperties.mqttTopicPrecision(MqttBrokerTarget.AV), messageType) : Set.of();
    Set<String> mbTopics = mbPublish ? NmiMqttTopicBuilder.getTimTopicListFromGeohashes(geohashes,
        etxProperties.mqttTopicPrecision(MqttBrokerTarget.MB), messageType) : Set.of();
    return new TimTopicSets(etxTopics, nmiTopics, avTopics, mbTopics);
  }

  private TimTopicSets buildTopicSetsFromDataFrames(TravelerDataFrameList dataFramesList,
      boolean etxPublish, boolean nmiPublish, boolean avPublish, boolean mbPublish) {
    Set<String> etxTopics =
        etxPublish ? EtxMqttTopicBuilder.getTimTopicList(dataFramesList, mqttProperties.getVendor(),
            mqttProperties.getPrecision(), mqttProperties.getMessageFormat(),
            etxProperties.getClientType(), etxProperties.getClientSubType()) : Set.of();
    Set<String> nmiTopics =
        nmiPublish
            ? NmiMqttTopicBuilder.getTimTopicList(dataFramesList,
                etxProperties.mqttTopicPrecision(MqttBrokerTarget.NMI), messageType)
            : Set.of();
    Set<String> avTopics =
        avPublish
            ? NmiMqttTopicBuilder.getTimTopicList(dataFramesList,
                etxProperties.mqttTopicPrecision(MqttBrokerTarget.AV), messageType)
            : Set.of();
    Set<String> mbTopics =
        mbPublish
            ? NmiMqttTopicBuilder.getTimTopicList(dataFramesList,
                etxProperties.mqttTopicPrecision(MqttBrokerTarget.MB), messageType)
            : Set.of();
    return new TimTopicSets(etxTopics, nmiTopics, avTopics, mbTopics);
  }

  private record TimTopicSets(Set<String> etxTopics, Set<String> nmiTopics, Set<String> avTopics,
      Set<String> mbTopics) {
  }
}
