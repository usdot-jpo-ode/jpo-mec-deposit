package us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt;

import java.util.Set;
import lombok.Builder;
import lombok.Value;

/**
 * Payload and topics for publishing to a specific broker.
 */
@Value
@Builder
public class BrokerPublishPayload {
  MqttBrokerTarget target;
  Set<String> topics;
  byte[] payload;
}
