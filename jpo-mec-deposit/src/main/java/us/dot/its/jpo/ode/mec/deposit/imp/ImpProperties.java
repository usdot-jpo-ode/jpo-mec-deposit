package us.dot.its.jpo.ode.mec.deposit.imp;

import lombok.Data;
import us.dot.its.jpo.ode.mec.deposit.imp.mqtt.ImpMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.imp.ImpClientSubType;
import us.dot.its.jpo.ode.mec.deposit.models.imp.ImpClientType;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.ImpNetworkType;

import java.math.BigDecimal;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "depositor.imp")
@Data
public class ImpProperties {
    private boolean enabled;
    private String vendor;
    private ImpNetworkType networkType;
    private boolean cacheRegistration;
    private String certificatePath;
    private ImpClientType clientType;
    private ImpClientSubType clientSubType;
    private PartnerApiProperties partnerApi;
    private ImpMqttProperties mqtt;

    @Data
    public static class PartnerApiProperties {
        private String baseUri;
        private String user;
        private String pass;
        private DepositorProperties depositors;
        private BigDecimal mecLatitude;
        private BigDecimal mecLongitude;
    }

    @Data
    public static class DepositorProperties {
        private MapProperties map;
        private TimProperties tim;
    }

    @Data
    public static class MapProperties {
        private String sourceKafkaTopic;
        private String distributionType;
    }

    @Data
    public static class TimProperties {
        private String sourceKafkaTopic;
        private String distributionType;
    }

    @Data
    public static class MqttDepositorProperties {
        private BsmProperties bsm;
        private SpatProperties spat;
        private TimProperties tim;
    }

    @Data
    public static class BsmProperties {
        private String sourceKafkaTopic;
    }

    @Data
    public static class SpatProperties {
        private String sourceKafkaTopic;
    }
}