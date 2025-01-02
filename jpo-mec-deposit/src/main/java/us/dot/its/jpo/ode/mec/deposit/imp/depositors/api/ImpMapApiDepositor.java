package us.dot.its.jpo.ode.mec.deposit.imp.depositors.api;

import lombok.extern.slf4j.Slf4j;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.MeterRegistry;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpApi;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpProperties;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpTokenManager;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttMessageType;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.AuthToken;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.DistributionType;
import us.dot.its.jpo.ode.model.OdeMapData;

/**
 * Depositor class for handling MAP messages via IMP API integration.
 */
@Component
@Slf4j
@ConditionalOnProperty(value = {"imp.depositors.map.api.enabled", "imp.enabled"},
    havingValue = "true")
public class ImpMapApiDepositor extends AbstractImpApiDepositor {
  private final DistributionType distributionType = DistributionType.TARGETED;

  public ImpMapApiDepositor(ImpProperties properties, MeterRegistry meterRegistry, ImpApi impApi,
      ImpTokenManager tokenManager) {
    super(properties, ImpMqttMessageType.MAP, meterRegistry, impApi, tokenManager);
  }


  /**
   * Listens for MAP messages on the configured Kafka topic and deposits them via the IMP API.
   *
   * @param message The MAP message to deposit
   */
  @Async("kafkaListenerExecutor")
  @KafkaListener(topics = "${imp.depositors.map.api.kafka-topic}",
      groupId = "${spring.kafka.consumer.group-id}-map-api-depositor",
      concurrency = "${listen.concurrency:1}", containerFactory = "kafkaListenerContainerFactory")
  public void mapDepositListener(String message) {
    try {
      LocalDateTime startTime = LocalDateTime.now(ZoneOffset.UTC);
      OdeMapData msg = mapper.readValue(message, OdeMapData.class);
      String odeReceivedAt = msg.getMetadata().getOdeReceivedAt();

      if (isMessageStale(odeReceivedAt)) {
        return;
      }

      String asn1String = msg.getMetadata().getAsn1();

      String token = tokenManager.getValidToken();
      log.info("Depositing MAP message to IMP API");
      this.partnerApi.deposit(token, asn1String, this.distributionType);
      recordLatency(odeReceivedAt, startTime);
    } catch (Exception e) {
      log.error("Error depositing MAP message", e);
    }
  }
}
