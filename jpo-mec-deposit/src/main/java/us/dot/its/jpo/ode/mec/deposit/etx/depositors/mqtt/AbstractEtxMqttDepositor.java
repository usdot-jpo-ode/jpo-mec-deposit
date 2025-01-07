package us.dot.its.jpo.ode.mec.deposit.etx.depositors.mqtt;

import io.micrometer.core.instrument.MeterRegistry;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.models.mqtt.EtxMqttMessageType;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttService;
import us.dot.its.jpo.ode.mec.deposit.etx.depositors.AbstractEtxDepositor;

public abstract class AbstractEtxMqttDepositor extends AbstractEtxDepositor {
  protected final EtxMqttService mqttService;

  protected AbstractEtxMqttDepositor(EtxProperties etxProperties, EtxMqttService mqttService,
      EtxMqttMessageType messageType, MeterRegistry registry) {
    super(etxProperties, messageType, registry, "etx.mqtt");
    this.mqttService = mqttService;
  }

  @Override
  protected String getProcessingType() {
    return "MQTT";
  }
}
