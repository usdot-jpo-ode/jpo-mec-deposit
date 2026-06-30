package us.dot.its.jpo.ode.mec.deposit.services.base;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.LocalDateTime;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.lang.Nullable;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxDepositorType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;
import us.dot.its.jpo.ode.mec.deposit.services.etx.EtxRegistrationRefreshService;
import us.dot.its.jpo.ode.mec.deposit.services.etx.mqtt.EtxMqttPublishService;

/**
 * Abstract base class for MQTT depositors. Extends {@link AbstractDepositor} to provide common MQTT
 * deposit functionality.
 */
@Slf4j
public abstract class AbstractMqttDepositor extends AbstractDepositor {
  @Nullable
  protected final EtxMqttPublishService mqttService;
  protected final EtxMqttProperties mqttProperties;

  @Autowired(required = false)
  protected EtxRegistrationRefreshService refreshService;

  protected AbstractMqttDepositor(MecDepositProperties mecDepositProperties,
      EtxProperties etxProperties, EtxMqttProperties mqttProperties, EtxMessageType messageType,
      @Nullable EtxMqttPublishService mqttService, MeterRegistry registry,
      KafkaTemplate<String, String> kafkaTemplate) {
    super(mecDepositProperties, etxProperties, messageType, registry, "mec-deposit.etx.mqtt",
        kafkaTemplate);
    this.mqttService = mqttService;
    this.mqttProperties = mqttProperties;
  }

  @Override
  protected EtxDepositorType getDepositorType() {
    return EtxDepositorType.MQTT;
  }

  /**
   * Overrides the parent's error handling to also record failures with the refresh service.
   *
   * @param e The exception that occurred
   * @param topics The topics that were attempted
   * @param odeReceivedAtMillis The timestamp when the message was received
   * @param asn1Hex The ASN.1 hex string
   */
  @Override
  protected void handleProcessingError(Exception e, Set<String> topics, long odeReceivedAtMillis,
      String asn1Hex) {
    if (refreshService != null) {
      boolean refreshTriggered = refreshService.recordFailure();
      if (refreshTriggered) {
        log.warn("Registration refresh was triggered due to MQTT publish failures");
      }
    }
    super.handleProcessingError(e, topics, odeReceivedAtMillis, asn1Hex);
  }

  /**
   * Overrides the parent's success handling to also record success with the refresh service.
   *
   * @param topics The topics where the message was published
   * @param odeReceivedAt The timestamp when the message was received
   * @param depositedAt The timestamp when the message was deposited
   * @param asn1Hex The ASN.1 hex string
   */
  @Override
  protected void handleProcessingSuccess(Set<String> topics, String odeReceivedAt,
      LocalDateTime depositedAt, String asn1Hex) {
    handleProcessingSuccess(topics, odeReceivedAt, depositedAt, asn1Hex, null);
  }

  @Override
  protected void handleProcessingSuccess(Set<String> topics, String odeReceivedAt,
      LocalDateTime depositedAt, String asn1Hex, @Nullable Set<String> mqttBrokerTargets) {
    if (refreshService != null) {
      refreshService.recordSuccess();
    }
    super.handleProcessingSuccess(topics, odeReceivedAt, depositedAt, asn1Hex, mqttBrokerTargets);
  }
}
