package us.dot.its.jpo.ode.mec.deposit.services.mb.mqtt;

import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.BrokerPublishPayload;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttBrokerTarget;
import us.dot.its.jpo.ode.mec.deposit.services.mqtt.BrokerPublisher;

/**
 * MB target publisher.
 */
@Component
public class MbBrokerPublisher implements BrokerPublisher {
  private final MbMqttPublishService mbMqttPublishService;

  public MbBrokerPublisher(MbMqttPublishService mbMqttPublishService) {
    this.mbMqttPublishService = mbMqttPublishService;
  }

  @Override
  public MqttBrokerTarget target() {
    return MqttBrokerTarget.MB;
  }

  @Override
  public void publish(BrokerPublishPayload payload, boolean retain) {
    for (String topic : payload.getTopics()) {
      mbMqttPublishService.publishAsn1Bytes(topic, payload.getPayload(), retain);
    }
  }
}
