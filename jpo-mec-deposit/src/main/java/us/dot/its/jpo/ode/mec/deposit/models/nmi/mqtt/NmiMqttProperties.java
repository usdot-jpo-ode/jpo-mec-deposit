package us.dot.its.jpo.ode.mec.deposit.models.nmi.mqtt;

import lombok.Data;

/**
 * NMI MQTT connection properties (bound under {@code mec-deposit.etx.mqtt-brokers.nmi.mqtt}).
 */
@Data
public class NmiMqttProperties {
  private boolean enabled = false;
  private String brokerUri;
  private String clientId = "jpo-mec-deposit-nmi";
  private int qos = 0;
  private int maxMessagesPerSecond = 100;
  private int precision = 7;
  private int circuitBreakerFailureThreshold = 10;
}
