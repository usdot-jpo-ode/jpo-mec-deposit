package us.dot.its.jpo.ode.mec.deposit.imp.mqtt;

import java.util.List;
import lombok.Data;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpProperties.MqttDepositorProperties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "depositor.imp.mqtt")
@Data
public class MqttProperties {
    private int qos;
    private String vendor;
    private MqttDepositorProperties depositors;
    private String[] subscriptions;
}
