package us.dot.its.jpo.ode.mec.deposit.services.etx.depositors.api;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxPartnerClient;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxTokenManager;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.DistributionType;
import us.dot.its.jpo.ode.mec.deposit.services.base.AbstractEtxApiDepositor;
import us.dot.its.jpo.ode.model.OdeTimData;

/**
 * Depositor class for handling TIM messages via ETX API integration.
 */
@Component
@Slf4j
@ConditionalOnProperty(
    value = {"mec-deposit.etx.depositors.tim.api.enabled", "mec-deposit.etx.enabled"},
    havingValue = "true")
public class EtxTimApiDepositor extends AbstractEtxApiDepositor {
  private final DistributionType distributionType;

  /**
   * Constructs a new EtxTimApiDepositor.
   *
   * @param mecDepositProperties Configuration properties for MEC deposit
   * @param etxProperties ETX-specific configuration properties
   * @param etxApi Service for interacting with ETX API
   * @param tokenManager Manager for ETX authentication tokens
   * @param meterRegistry Registry for metrics collection
   * @param kafkaTemplate Template for Kafka operations
   */
  public EtxTimApiDepositor(MecDepositProperties mecDepositProperties, EtxProperties etxProperties,
      EtxPartnerClient etxApi, EtxTokenManager tokenManager, MeterRegistry meterRegistry,
      KafkaTemplate<String, String> kafkaTemplate) {
    super(mecDepositProperties, etxProperties, etxApi, tokenManager, meterRegistry, kafkaTemplate,
        EtxMessageType.TIM);
    this.distributionType = etxProperties.getDepositors().getTim().getApi().getDistributionType();
  }


  /**
   * Listens for TIM messages on the configured Kafka topic and deposits them via the ETX API.
   *
   * @param message The TIM message to deposit
   */
  @Async("kafkaListenerExecutor")
  @KafkaListener(topics = "${mec-deposit.etx.depositors.tim.api.kafka-topic}",
      groupId = "${spring.kafka.consumer.group-id}-tim-api-depositor",
      concurrency = "${listen.concurrency:1}", containerFactory = "kafkaListenerContainerFactory")
  public void timDepositListener(String message) {
    String odeReceivedAt = null;
    String asn1Hex = "";
    try {
      OdeTimData msg = mapper.readValue(message, OdeTimData.class);
      odeReceivedAt = msg.getMetadata().getOdeReceivedAt();

      asn1Hex = msg.getMetadata().getAsn1();

      String token = tokenManager.getValidToken();
      log.info("Depositing TIM message to ETX API");

      LocalDateTime depositedAt = LocalDateTime.now(ZoneOffset.UTC);
      partnerApi.deposit(token, asn1Hex, distributionType);

      handleProcessingSuccess(null, distributionType, odeReceivedAt, depositedAt, asn1Hex);
    } catch (Exception e) {
      handleProcessingError(e, null, distributionType,
          odeReceivedAt != null ? Instant.parse(odeReceivedAt).toEpochMilli() : 0, asn1Hex);
    }
  }
}

