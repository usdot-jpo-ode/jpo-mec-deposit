package us.dot.its.jpo.ode.mec.deposit.etx;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientSubType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.DistributionType;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxPartnerApiProperties;


/**
 * Configuration properties for ETX integration. Defines properties for API endpoints, MQTT
 * settings, and client configuration.
 */
@Configuration
@ConfigurationProperties(prefix = "mec-deposit.etx")
@Data
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
  public static class EtxDepositors {
    private ImpDepositorProperties bsm;
    private ImpDepositorProperties spat;
    private ImpDepositorProperties tim;
    private ImpDepositorProperties map;
  }

  /**
   * Properties for ETX depositor configuration.
   */
  @Data
  public static class ImpDepositorProperties {
    private EtxMqttDepositorProperties mqtt;
    private ImpApiDepositorProperties api;
  }

  /**
   * Properties for the ETX Mqtt configuration.
   */
  @Data
  public static class EtxMqttDepositorProperties {
    private Boolean enabled;
    private String kafkaTopic;
  }

  /**
   * Properties for the ETX Api configuration.
   */
  @Data
  public static class ImpApiDepositorProperties {
    private Boolean enabled;
    private String kafkaTopic;
    private DistributionType distributionType;
  }
}
