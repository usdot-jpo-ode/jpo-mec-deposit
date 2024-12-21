package us.dot.its.jpo.ode.mec.deposit.imp.mqtt;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttMessageFormat;

/**
 * Configuration properties for IMP MQTT connection and messaging settings.
 */
@Configuration
@ConfigurationProperties(prefix = "depositor.imp.mqtt")
@Data
public class ImpMqttProperties {
  private int qos;
  private String vendor;
  private String[] subscriptions;
  private int maxInflight;
  private int connectionTimeout;
  private int keepAliveInterval;
  private int completionTimeout;
  private ImpMqttMessageFormat messageFormat;
  private int maxMessagesPerSecond;
  private int staleMessageThreshold;

}
