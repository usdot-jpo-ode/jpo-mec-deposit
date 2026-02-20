package us.dot.its.jpo.ode.mec.deposit.etx;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxPartnerApiProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientSubType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientType;
import java.util.List;

/**
 * Configuration properties for ETX integration. Defines properties for API endpoints, MQTT
 * settings, and client configuration.
 */
@Configuration
@ConfigurationProperties(prefix = "mec-deposit.etx")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EtxProperties {
  private boolean enabled;
  private EtxClientType clientType;
  private EtxClientSubType clientSubType;
  private EtxPartnerApiProperties partnerApi;
  private EtxMqttProperties mqtt;
  private EtxDepositors depositors;


  /**
   * Properties for ETX depositor configuration groups.
   */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class EtxDepositors {
    private int staleMessageThreshold;
    private DepositorProperties bsm;
    private DepositorProperties spat;
    private DepositorProperties tim;
    private DepositorProperties map;
  }

  /**
   * Properties for ETX depositor configuration.
   */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class DepositorProperties {
    private MqttDepositorProperties mqtt;
    private ApiDepositorProperties api;
  }

  /**
   * Properties for the ETX Mqtt configuration.
   */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class MqttDepositorProperties {
    private Boolean enabled;
    private String kafkaTopic;
    private SpatIntersectionFilterProperties intersectionFilter;
  }

  /**
   * Properties for the SPAT intersection filter configuration.
   */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class SpatIntersectionFilterProperties {
    private Boolean enabled;
    private List<Long> allowedIntersectionIds;
    private List<Long> blockedIntersectionIds;
  }

  /**
   * Properties for the ETX Api configuration.
   */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ApiDepositorProperties {
    private Boolean enabled;
    private String kafkaTopic;
  }
}
