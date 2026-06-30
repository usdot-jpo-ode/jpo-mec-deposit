package us.dot.its.jpo.ode.mec.deposit.services.mqtt;

import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.BrokerPublishPayload;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttBrokerTarget;

/**
 * Publishes MQTT payloads to a specific broker target.
 */
public interface BrokerPublisher {
  MqttBrokerTarget target();

  void publish(BrokerPublishPayload payload, boolean retain);
}
