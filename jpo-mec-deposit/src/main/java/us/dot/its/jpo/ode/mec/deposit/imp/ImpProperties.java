package us.dot.its.jpo.ode.mec.deposit.imp;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "depositor.imp")
@Data
public class ImpProperties {
    private boolean enabled;
    private String vendor;
    private String networkType;
    private boolean cacheRegistration;
    private String certificatePath;
    private PartnerApiProperties partnerApi;
    private MecProperties mec;

    @Data
    public static class PartnerApiProperties {
        private String baseUri;
        private String user;
        private String pass;
        private DepositorProperties depositors;
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
    public static class MecProperties {
        private String latitude;
        private String longitude;
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