package us.dot.its.jpo.ode.mec.deposit.services.nmi.mqtt;

import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.BrokerPublishPayload;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttBrokerTarget;
import us.dot.its.jpo.ode.mec.deposit.services.mqtt.BrokerPublisher;

/**
 * NMI target publisher.
 */
@Component
public class NmiBrokerPublisher implements BrokerPublisher {
  private final NmiMqttPublishService nmiMqttPublishService;

  public NmiBrokerPublisher(NmiMqttPublishService nmiMqttPublishService) {
    this.nmiMqttPublishService = nmiMqttPublishService;
  }

  @Override
  public MqttBrokerTarget target() {
    return MqttBrokerTarget.NMI;
  }

  @Override
  public void publish(BrokerPublishPayload payload, boolean retain) {
    for (String topic : payload.getTopics()) {
      nmiMqttPublishService.publishAsn1Bytes(topic, payload.getPayload(), retain);
    }
  }
}
