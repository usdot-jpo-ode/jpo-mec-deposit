package us.dot.its.jpo.ode.mec.deposit.services.depositors.mqtt;

import ch.hsr.geohash.GeoHash;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
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
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttBrokerTarget;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttFanoutPublishResult;
import us.dot.its.jpo.ode.mec.deposit.services.base.AbstractMqttDepositor;
import us.dot.its.jpo.ode.mec.deposit.services.etx.mqtt.EtxBrokerPublisher;
import us.dot.its.jpo.ode.mec.deposit.services.etx.mqtt.EtxMqttPublishService;
import us.dot.its.jpo.ode.mec.deposit.services.mqtt.MultiBrokerPublishService;
import us.dot.its.jpo.ode.mec.deposit.utils.GeohashUtils;
import us.dot.its.jpo.ode.mec.deposit.utils.MessageTypeDetector;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttProtobufBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.EtxMqttTopicBuilder;
import us.dot.its.jpo.ode.mec.deposit.utils.mqtt.NmiMqttTopicBuilder;
import us.dot.its.jpo.ode.plugin.j2735.OdeTravelerInformationMessage.DataFrame.Region.Geometry;
import us.dot.its.jpo.ode.rtk_asn1_bridge.models.Coordinate;
import us.dot.its.jpo.ode.rtk_asn1_bridge.models.MsgType;
import us.dot.its.jpo.ode.rtk_asn1_bridge.models.PolygonMessage;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

/**
 * Publisher for PolygonMessage protobuf payloads consumed from Kafka and fanned out to active MQTT
 * broker targets.
 */
@Component
@Slf4j
@ConditionalOnAnyMqttBrokerMqttDepositor("polygon")
public class PolygonMqttPublisher extends AbstractMqttDepositor {
  private final MultiBrokerPublishService multiBrokerPublishService;
  private final GeohashUtils geohashUtils;
  private final Map<Polygon, Set<String>> polygonGeohashCache = new HashMap<>();

  private final int gridPrecision = 7;
  private final int gridSize = 3;

  private double latStep;
  private double lonStep;


  @Autowired
  public PolygonMqttPublisher(MecDepositProperties mecDepositProperties,
      EtxProperties etxProperties, EtxMqttProperties mqttProperties,
      @Nullable EtxMqttPublishService mqttService, MeterRegistry registry,
      MultiBrokerPublishService multiBrokerPublishService,
      KafkaTemplate<String, String> kafkaTemplate, GeohashUtils geohashUtils) {
    super(mecDepositProperties, etxProperties, mqttProperties, EtxMessageType.TIM, mqttService,
        registry, kafkaTemplate);
    this.multiBrokerPublishService = multiBrokerPublishService;
    this.geohashUtils = geohashUtils;
    preComputeSteps();

  }

  /**
   * Backward-compatible constructor used by unit tests.
   */
  public PolygonMqttPublisher(MecDepositProperties mecDepositProperties,
      EtxProperties etxProperties, EtxMqttProperties mqttProperties,
      EtxMqttPublishService mqttService, MeterRegistry registry,
      KafkaTemplate<String, String> kafkaTemplate) {
    this(mecDepositProperties, etxProperties, mqttProperties, mqttService, registry,
        new MultiBrokerPublishService(List.of(new EtxBrokerPublisher(mqttService)),
            Set.of(MqttBrokerTarget.ETX), registry),
        kafkaTemplate, new GeohashUtils());
    preComputeSteps();
  }

  private void preComputeSteps() {
    // Pre-calculate step sizes for performance
    latStep = geohashUtils.calculateLatStep(gridPrecision);
    lonStep = geohashUtils.calculateLonStep(gridPrecision);
  }

  /**
   * Listens for PolygonMessage protobuf payloads and fans out to configured MQTT brokers.
   */
  @KafkaListener(topics = "${mec-deposit.etx.mqtt-brokers.etx.depositors.polygon.mqtt.kafka-topic}",
      groupId = "${spring.kafka.consumer.group-id}-polygon-mqtt-publisher",
      concurrency = "${spring.kafka.listener.concurrency:1}",
      containerFactory = "byteArrayKafkaListenerContainerFactory")
  public void polygonPublishListener(byte[] polygonMessageBytes) {
    boolean retain = false;
    Set<String> topicSet = null;
    String asn1Hex = "";
    LocalDateTime depositedAt = LocalDateTime.now(ZoneOffset.UTC);



    try {
      PolygonMessage polygonMessage = PolygonMessage.parseFrom(polygonMessageBytes);
      Instant messageTime =
          resolveMessageTime(polygonMessage, depositedAt.toInstant(ZoneOffset.UTC));
      if (isMessageStale(messageTime.getEpochSecond(), messageTime.getNano())) {
        log.info("Skipping Message as it is stale. Message Time: " + messageTime
            + ", Deposited At: " + depositedAt);
        return;
      }

      byte[] originalMessageBytes = polygonMessage.getMsgBytes().toByteArray();
      asn1Hex = Hex.toHexString(originalMessageBytes);

      EtxMessageType detectedMessageType = mapPolygonMessageType(polygonMessage.getMsgType());
      String dsrcMsgId = MessageTypeDetector.getDsrcMsgIdForMessageType(detectedMessageType);
      if (dsrcMsgId == null || dsrcMsgId.isBlank()) {
        throw new IllegalArgumentException(
            "No DSRC message id mapping for polygon message type " + detectedMessageType);
      }

      Set<String> geohashes =
          resolvePolygonGeohashesTemplate(polygonMessage, mqttProperties.getPrecision());
      if (geohashes.isEmpty()) {
        throw new IllegalArgumentException("No geohashes were resolved for PolygonMessage");
      }



      Coordinate representative = resolveRepresentativeCoordinate(polygonMessage, geohashes);
      Instant depositedInstant = depositedAt.toInstant(ZoneOffset.UTC);
      byte[] etxPayload = EtxMqttProtobufBuilder.toEtxMqttWirePayload(originalMessageBytes,
          mqttProperties.getMessageFormat(), depositedInstant, representative.getLatitude(),
          representative.getLongitude());

      Map<MqttBrokerTarget, BrokerPublishPayload> payloads = new EnumMap<>(MqttBrokerTarget.class);
      EtxMqttNamespace namespace =
          retain ? EtxMqttNamespace.REGIONAL_STATIC : EtxMqttNamespace.REGIONAL;
      Set<String> etxTopics = new LinkedHashSet<>();
      for (String geohash : geohashes) {
        etxTopics.add(EtxMqttTopicBuilder.buildTopicFromGeohash(namespace, geohash,
            detectedMessageType, mqttProperties.getPrecision(), mqttProperties.getVendor(),
            mqttProperties.getMessageFormat(), etxProperties.getClientType(),
            etxProperties.getClientSubType()));
      }
      if (multiBrokerPublishService.isTargetActive(MqttBrokerTarget.ETX)
          && etxProperties.isMqttDepositorEnabled(MqttBrokerTarget.ETX, "polygon")
          && !etxTopics.isEmpty()) {
        payloads.put(MqttBrokerTarget.ETX, BrokerPublishPayload.builder()
            .target(MqttBrokerTarget.ETX).topics(etxTopics).payload(etxPayload).build());
      }

      addNmiCompatiblePayloadForTarget(payloads, MqttBrokerTarget.NMI, geohashes, dsrcMsgId,
          originalMessageBytes);
      addNmiCompatiblePayloadForTarget(payloads, MqttBrokerTarget.AV, geohashes, dsrcMsgId,
          originalMessageBytes);
      addNmiCompatiblePayloadForTarget(payloads, MqttBrokerTarget.MB, geohashes, dsrcMsgId,
          originalMessageBytes);

      topicSet = MultiBrokerPublishService.unionPayloadTopics(payloads);
      // MqttFanoutPublishResult fanout = multiBrokerPublishService.publish(payloads, retain);
      // Set<String> metricTopics =
      // fanout.publishedTopics().isEmpty() ? topicSet : fanout.publishedTopics();

      // log.debug("Sending polygon payload with {} geohash topics (detected type: {})",
      // geohashes.size(), detectedMessageType);
      // handleProcessingSuccess(metricTopics, messageTime.toString(), depositedAt, asn1Hex,
      // fanout.mqttBrokerTargets().isEmpty() ? null : fanout.mqttBrokerTargets(),
      // detectedMessageType);
    } catch (Exception e) {
      handleProcessingError(e, topicSet, depositedAt.toInstant(ZoneOffset.UTC).toEpochMilli(),
          asn1Hex);
    }
  }

  /**
   * Template function for Polygon-to-Geohash conversion strategy. TODO: Replace this placeholder
   * implementation with the production polygon covering algorithm.
   */
  private Set<String> resolvePolygonGeohashesTemplate(PolygonMessage polygonMessage,
      int precision) {


    Polygon polygon = geohashUtils.convertPolygonMessageToPolygon(polygonMessage);

    polygonGeohashCache.computeIfAbsent(polygon, p -> {
      Set<String> affectedGeohashes = new HashSet<>();
      List<String> representativeGeohashes = new ArrayList<>();
      Set<String> existingUsedGeohashes = null;

      geohashUtils.processPolygon(polygon, gridPrecision, gridSize, latStep, lonStep,
          affectedGeohashes, representativeGeohashes, existingUsedGeohashes);

      return new HashSet<>(representativeGeohashes);
    });



    return polygonGeohashCache.get(polygon);
  }

  private void addNmiCompatiblePayloadForTarget(
      Map<MqttBrokerTarget, BrokerPublishPayload> payloads, MqttBrokerTarget target,
      Set<String> geohashes, String dsrcMsgId, byte[] originalMessageBytes) {
    if (!multiBrokerPublishService.isTargetActive(target)
        || !etxProperties.isMqttDepositorEnabled(target, "polygon")) {
      return;
    }

    Set<String> topics = new LinkedHashSet<>();
    int targetPrecision = etxProperties.mqttTopicPrecision(target);
    for (String geohash : geohashes) {
      topics.add(NmiMqttTopicBuilder.buildTopicFromGeohash(geohash, targetPrecision, dsrcMsgId));
    }

    if (!topics.isEmpty()) {
      payloads.put(target, BrokerPublishPayload.builder().target(target).topics(topics)
          .payload(originalMessageBytes).build());
    }
  }

  private Instant resolveMessageTime(PolygonMessage polygonMessage, Instant fallback) {
    if (polygonMessage.hasTime()) {
      return Instant.ofEpochSecond(polygonMessage.getTime().getSeconds(),
          polygonMessage.getTime().getNanos());
    }
    return fallback;
  }

  private Coordinate resolveRepresentativeCoordinate(PolygonMessage polygonMessage,
      Set<String> geohashes) {
    if (polygonMessage.hasCoordinate()) {
      return polygonMessage.getCoordinate();
    }
    String firstGeohash = geohashes.iterator().next();
    GeoHash geohashObject = GeoHash.fromGeohashString(firstGeohash);
    return Coordinate.newBuilder().setLatitude(geohashObject.getOriginatingPoint().getLatitude())
        .setLongitude(geohashObject.getOriginatingPoint().getLongitude()).build();
  }

  private EtxMessageType mapPolygonMessageType(MsgType polygonMsgType) {
    return switch (polygonMsgType) {
      case BSM -> EtxMessageType.BSM;
      case MAP -> EtxMessageType.MAP;
      case SPAT -> EtxMessageType.SPAT;
      case TIM -> EtxMessageType.TIM;
      case PSM -> EtxMessageType.PSM;
      case SDSM -> EtxMessageType.SDSM;
      case RTCM -> EtxMessageType.RTCM;
      case TAM -> EtxMessageType.TAM;
      case TUM -> EtxMessageType.TUM;
      case TUMAck -> EtxMessageType.TUMAck;
      case SSM -> EtxMessageType.SSM;
      case SRM -> EtxMessageType.SRM;
      default -> throw new IllegalArgumentException(
          "Unsupported PolygonMessage msgType for ETX routing: " + polygonMsgType);
    };
  }



}
