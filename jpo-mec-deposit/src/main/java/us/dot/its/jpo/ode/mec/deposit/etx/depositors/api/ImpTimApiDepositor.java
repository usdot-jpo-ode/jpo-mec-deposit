package us.dot.its.jpo.ode.mec.deposit.etx.depositors.api;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxApi;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxTokenManager;
import us.dot.its.jpo.ode.mec.deposit.etx.models.mqtt.EtxMqttMessageType;
import us.dot.its.jpo.ode.mec.deposit.etx.models.partner.DistributionType;
import us.dot.its.jpo.ode.model.OdeTimData;

/**
 * Depositor class for handling TIM messages via ETX API integration.
 */
@Component
@Slf4j
@ConditionalOnProperty(value = {"etx.depositors.tim.api.enabled", "etx.enabled"},
    havingValue = "true")
public class ImpTimApiDepositor extends AbstractImpApiDepositor {
  private final DistributionType distributionType = DistributionType.TARGETED;

  public ImpTimApiDepositor(EtxProperties properties, MeterRegistry meterRegistry, EtxApi impApi,
      EtxTokenManager tokenManager) {
    super(properties, EtxMqttMessageType.TIM, meterRegistry, impApi, tokenManager);
  }


  /**
   * Listens for TIM messages on the configured Kafka topic and deposits them via the ETX API.
   *
   * @param message The TIM message to deposit
   */
  @Async("kafkaListenerExecutor")
  @KafkaListener(topics = "${etx.depositors.tim.api.kafka-topic}",
      groupId = "${spring.kafka.consumer.group-id}-tim-api-depositor",
      concurrency = "${listen.concurrency:1}", containerFactory = "kafkaListenerContainerFactory")
  public void timDepositListener(String message) {
    try {
      final LocalDateTime startTime = LocalDateTime.now(ZoneOffset.UTC);
      OdeTimData msg = mapper.readValue(message, OdeTimData.class);
      String odeReceivedAt = msg.getMetadata().getOdeReceivedAt();

      if (isMessageStale(odeReceivedAt)) {
        return;
      }

      String asn1String = msg.getMetadata().getAsn1();

      String token = tokenManager.getValidToken();
      log.info("Depositing TIM message to ETX API");
      this.partnerApi.deposit(token, asn1String, this.distributionType);
      recordLatency(odeReceivedAt, startTime);
    } catch (Exception e) {
      log.error("Error depositing TIM message", e);
    }
  }
}
