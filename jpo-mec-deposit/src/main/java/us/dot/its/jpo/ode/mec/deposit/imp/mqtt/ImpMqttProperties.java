package us.dot.its.jpo.ode.mec.deposit.imp.mqtt;

import lombok.Data;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpProperties.MqttDepositorProperties;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttMessageType;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "depositor.imp.mqtt")
@Data
public class ImpMqttProperties {
    private int qos;
    private String vendor;
    private MqttDepositorProperties depositors;
    private String[] subscriptions;
    private int maxInflight;
    private ImpMqttMessageFormat messageFormat;
}
