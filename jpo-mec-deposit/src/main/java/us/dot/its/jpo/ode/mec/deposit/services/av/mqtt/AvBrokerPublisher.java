package us.dot.its.jpo.ode.mec.deposit.services.av.mqtt;

import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.BrokerPublishPayload;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttBrokerTarget;
import us.dot.its.jpo.ode.mec.deposit.services.mqtt.BrokerPublisher;

/**
 * AV target publisher.
 */
@Component
public class AvBrokerPublisher implements BrokerPublisher {
  private final AvMqttPublishService avMqttPublishService;

  public AvBrokerPublisher(AvMqttPublishService avMqttPublishService) {
    this.avMqttPublishService = avMqttPublishService;
  }

  @Override
  public MqttBrokerTarget target() {
    return MqttBrokerTarget.AV;
  }

  @Override
  public void publish(BrokerPublishPayload payload, boolean retain) {
    for (String topic : payload.getTopics()) {
      avMqttPublishService.publishAsn1Bytes(topic, payload.getPayload(), retain);
    }
  }
}
