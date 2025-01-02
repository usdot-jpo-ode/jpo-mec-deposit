package us.dot.its.jpo.ode.mec.deposit.imp.mqtt;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttMessageFormat;

/**
 * Configuration properties for IMP MQTT connection and messaging settings.
 */
@Configuration
@ConfigurationProperties(prefix = "imp.mqtt")
@Data
public class ImpMqttProperties {
  private int qos;
  private int maxInflight;
  private int connectionTimeout;
  private int keepAliveInterval;
  private int completionTimeout;
  private int maxMessagesPerSecond;
  private int staleMessageThreshold;
  private String vendor;
  private ImpMqttMessageFormat messageFormat;
  private String[] subscriptions;
}
