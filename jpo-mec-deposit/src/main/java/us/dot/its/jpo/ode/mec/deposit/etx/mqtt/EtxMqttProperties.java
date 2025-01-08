package us.dot.its.jpo.ode.mec.deposit.etx.mqtt;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import us.dot.its.jpo.ode.mec.deposit.etx.models.mqtt.EtxMqttMessageFormat;

/**
 * Configuration properties for ETX MQTT connection and messaging settings.
 */
@Configuration
@ConfigurationProperties(prefix = "etx.mqtt")
@Data
public class EtxMqttProperties {
  private int qos;
  private int maxInflight;
  private int connectionTimeout;
  private int keepAliveInterval;
  private int completionTimeout;
  private int maxMessagesPerSecond;
  private int staleMessageThreshold;
  private String vendor;
  private EtxMqttMessageFormat messageFormat;
  private String[] subscriptions;
}
