package us.dot.its.jpo.ode.mec.deposit.models.av.mqtt;

import lombok.Data;

/**
 * AV MQTT connection properties (bound under {@code mec-deposit.etx.mqtt-brokers.av.mqtt}).
 */
@Data
public class AvMqttProperties {
  private boolean enabled = false;
  private String brokerUri;
  private String clientId = "jpo-mec-deposit-av";
  private int qos = 0;
  private int maxMessagesPerSecond = 100;
  private int precision = 7;
  private int circuitBreakerFailureThreshold = 10;
}
