package us.dot.its.jpo.ode.mec.deposit.services.base;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxDepositorType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;
import us.dot.its.jpo.ode.mec.deposit.services.etx.EtxMqttPublishService;

/**
 * Abstract base class for ETX MQTT depositors. Extends AbstractEtxDepositor to provide common MQTT
 * deposit functionality.
 */
public abstract class AbstractEtxMqttDepositor extends AbstractEtxDepositor {
  protected final EtxMqttPublishService mqttService;
  protected final EtxMqttProperties mqttProperties;

  protected AbstractEtxMqttDepositor(MecDepositProperties mecDepositProperties,
      EtxProperties etxProperties, EtxMqttProperties mqttProperties, EtxMessageType messageType,
      EtxMqttPublishService mqttService, MeterRegistry registry,
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
}
