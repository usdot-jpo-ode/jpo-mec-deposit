package us.dot.its.jpo.ode.mec.deposit.imp.depositors.mqtt;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpProperties;
import us.dot.its.jpo.ode.mec.deposit.imp.mqtt.ImpMqttService;
import us.dot.its.jpo.ode.mec.deposit.imp.mqtt.ImpMqttTopicBuilder;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttMessageType;
import us.dot.its.jpo.ode.mec.deposit.utils.MapRefPointCollector;
import us.dot.its.jpo.ode.model.OdeTimData;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.TravelerInformation;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.GeographicalPath;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.TravelerDataFrame;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.TravelerDataFrameList;
import us.dot.its.jpo.ode.plugin.j2735.OdePosition3D;
import us.dot.its.jpo.ode.plugin.j2735.OdeTravelerInformationMessage;
import us.dot.its.jpo.ode.plugin.j2735.OdeTravelerInformationMessage.DataFrame;
import us.dot.its.jpo.ode.plugin.j2735.OdeTravelerInformationMessage.DataFrame.Region;
import us.dot.its.jpo.ode.plugin.j2735.common.Position3D;

/**
 * Depositor class for handling TIM messages via MQTT integration with IMP.
 */
@Component
@Slf4j
public class ImpTimMqttDepositor extends AbstractImpMqttDepositor {

  public ImpTimMqttDepositor(ImpProperties impProperties, ImpMqttService mqttService,
      MeterRegistry registry) {
    super(impProperties, mqttService, ImpMqttMessageType.TIM, registry);
  }

  /**
   * Listens for TIM messages from Kafka and deposits them to appropriate MQTT topics.
   *
   * @param message The TIM message from Kafka in JSON format
   */
  @ConditionalOnProperty(value = {"depositor.tim.enabled", "depositor.imp.enabled"},
      havingValue = "true")
  @Async("kafkaListenerExecutor")
  @KafkaListener(topics = "${depositor.tim.source-mqtt-kafka-topic}",
      groupId = "${spring.kafka.consumer.group-id}-tim-mqtt-depositor",
      concurrency = "${listen.concurrency:1}", containerFactory = "kafkaListenerContainerFactory")
  public void timDepositListener(String message) {
    boolean retain = false;
    try {
      final LocalDateTime startTime = LocalDateTime.now(ZoneOffset.UTC);
      OdeTimData msg = mapper.readValue(message, OdeTimData.class);

      String odeReceivedAt = msg.getMetadata().getOdeReceivedAt();
      if (isMessageStale(odeReceivedAt)) {
        return;
      }

      String asn1String = msg.getMetadata().getAsn1();

      var timMsg = (TravelerInformation) msg.getPayload().getData();
      var dataFramesList = timMsg.getDataFrames();
      List<String> topicList = new ArrayList<>();

      for (TravelerDataFrame dataFrame : dataFramesList) {
        var regions = dataFrame.getRegions();
        for (GeographicalPath region : regions) {
          Position3D refPoint = region.getAnchor();
          if (refPoint == null) {
            log.warn("No refPoint found for region: {} skipping IMP deposit", region.getName());
            continue;
          }
          // Convert from J2735 integer microdegrees to decimal degrees
          double scale = 10000000.0;
          double latitude = refPoint.getLat().getValue() / scale; // 38.9549122
          double longitude = refPoint.getLong_().getValue() / scale; // -77.1490570
          String topic = ImpMqttTopicBuilder.buildRegionalTopic(messageType, latitude, longitude, 7,
              impProperties);

          topicList.add(topic);
        }
      }

      byte[] asn1Bytes = Hex.decode(asn1String);

      for (String topic : topicList) {
        mqttService.publishAsn1Bytes(topic, asn1Bytes, retain);
        log.debug("Sending TIM message to MQTT topics: {}", topic);
      }

      recordLatency(odeReceivedAt, startTime);

    } catch (

    Exception e) {
      log.error("Error processing TIM message", e);
    }
  }
}

