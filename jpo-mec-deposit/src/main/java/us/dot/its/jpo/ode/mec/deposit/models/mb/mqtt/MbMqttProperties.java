package us.dot.its.jpo.ode.mec.deposit.models.mb.mqtt;

import lombok.Data;

/**
 * MB MQTT connection properties (bound under {@code mec-deposit.etx.mqtt-brokers.mb.mqtt}).
 */
@Data
public class MbMqttProperties {
  private boolean enabled = false;
  private String brokerUri;
  private String clientId = "jpo-mec-deposit-mb";
  private String username;
  private String password;
  private int qos = 0;
  private int maxMessagesPerSecond = 100;
  private int precision = 7;
  private int circuitBreakerFailureThreshold = 10;
}
