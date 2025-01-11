package us.dot.its.jpo.ode.mec.deposit.etx.depositors.mqtt;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.depositors.AbstractEtxDepositor;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxDepositorType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttService;

/**
 * Abstract base class for ETX MQTT depositors. Extends AbstractEtxDepositor to provide common MQTT
 * deposit functionality.
 */
public abstract class AbstractEtxMqttDepositor extends AbstractEtxDepositor {
  protected final EtxMqttService mqttService;

  protected AbstractEtxMqttDepositor(MecDepositProperties mecDepositProperties,
      EtxProperties etxProperties, EtxMessageType messageType, EtxMqttService mqttService,
      MeterRegistry registry, KafkaTemplate<String, String> kafkaTemplate) {
    super(mecDepositProperties, etxProperties, messageType, registry, "mec-deposit.etx.mqtt",
        kafkaTemplate);
    this.mqttService = mqttService;
  }

  @Override
  protected EtxDepositorType getDepositorType() {
    return EtxDepositorType.MQTT;
  }
}
