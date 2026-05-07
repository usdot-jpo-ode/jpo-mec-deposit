package us.dot.its.jpo.ode.mec.deposit.services.etx.mqtt;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.BrokerPublishPayload;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttBrokerTarget;
import us.dot.its.jpo.ode.mec.deposit.services.etx.EtxMqttPublishService;
import us.dot.its.jpo.ode.mec.deposit.services.mqtt.BrokerPublisher;

/**
 * ETX target publisher.
 */
@Component
@ConditionalOnProperty(value = {"mec-deposit.etx.enabled"}, havingValue = "true")
public class EtxBrokerPublisher implements BrokerPublisher {
  private final EtxMqttPublishService etxMqttPublishService;

  public EtxBrokerPublisher(EtxMqttPublishService etxMqttPublishService) {
    this.etxMqttPublishService = etxMqttPublishService;
  }

  @Override
  public MqttBrokerTarget target() {
    return MqttBrokerTarget.ETX;
  }

  @Override
  public void publish(BrokerPublishPayload payload, boolean retain) {
    for (String topic : payload.getTopics()) {
      etxMqttPublishService.publishAsn1Bytes(topic, payload.getPayload(), retain);
    }
  }
}
