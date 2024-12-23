package us.dot.its.jpo.ode.mec.deposit.imp.depositors.mqtt;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpProperties;
import us.dot.its.jpo.ode.mec.deposit.imp.mqtt.ImpMqttService;
import us.dot.its.jpo.ode.mec.deposit.imp.mqtt.ImpMqttTopicBuilder;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttMessageType;
import us.dot.its.jpo.ode.model.OdeTimData;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.TravelerInformation;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.GeographicalPath;
import us.dot.its.jpo.ode.plugin.j2735.travelerinformation.TravelerDataFrame;
import us.dot.its.jpo.ode.plugin.j2735.common.Position3D;

/**
 * Depositor class for handling TIM messages via MQTT integration with IMP.
 */
@Component
@Slf4j
@ConditionalOnProperty(value = {"depositor.tim.mqtt.enabled", "depositor.imp.enabled"},
    havingValue = "true")
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
  @Async("kafkaListenerExecutor")
  @KafkaListener(topics = "${depositor.tim.mqtt.kafka-topic}",
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
      List<String> topicList = ImpMqttTopicBuilder.getTimTopicList(dataFramesList, impProperties);

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

