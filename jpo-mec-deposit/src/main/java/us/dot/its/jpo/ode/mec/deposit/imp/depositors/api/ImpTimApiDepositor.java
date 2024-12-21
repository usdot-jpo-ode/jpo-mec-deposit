package us.dot.its.jpo.ode.mec.deposit.imp.depositors.api;

import lombok.extern.slf4j.Slf4j;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.MeterRegistry;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpProperties;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttMessageType;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.AuthToken;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.DistributionType;
import us.dot.its.jpo.ode.model.OdeTimData;

/**
 * Depositor class for handling TIM messages via IMP API integration.
 */
@Component
@Slf4j
public class ImpTimApiDepositor extends AbstractImpApiDepositor {
  private final DistributionType distributionType = DistributionType.TARGETED;

  public ImpTimApiDepositor(ImpProperties properties, MeterRegistry meterRegistry) {
    super(properties, ImpMqttMessageType.TIM, meterRegistry);
  }


  /**
   * Listens for TIM messages on the configured Kafka topic and deposits them via the IMP API.
   *
   * @param message The TIM message to deposit
   */
  @ConditionalOnProperty(value = {"depositor.tim.enabled", "depositor.imp.enabled"},
      havingValue = "true")
  @Async("kafkaListenerExecutor")
  @KafkaListener(topics = "${depositor.tim.source-api-kafka-topic}",
      groupId = "${spring.kafka.consumer.group-id}-tim-api-depositor",
      concurrency = "${listen.concurrency:1}", containerFactory = "kafkaListenerContainerFactory")
  public void timDepositListener(String message) {
    try {
      LocalDateTime startTime = LocalDateTime.now(ZoneOffset.UTC);
      OdeTimData msg = mapper.readValue(message, OdeTimData.class);
      String odeReceivedAt = msg.getMetadata().getOdeReceivedAt();

      if (isMessageStale(odeReceivedAt)) {
        return;
      }

      AuthToken authToken = getAuthToken();
      String asn1String = msg.getMetadata().getAsn1();

      this.partnerApi.deposit(authToken.getAccessToken(), asn1String, this.distributionType);
      recordLatency(odeReceivedAt, startTime);
    } catch (Exception e) {
      log.error("Error depositing TIM message", e);
    }
  }
}
