package us.dot.its.jpo.ode.mec.deposit.imp;

import java.util.List;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "depositor.imp.mqtt")
@Data
public class ImpMqttProperties {

    private List<String> subscriptions;
    private int qos;

}
