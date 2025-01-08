package us.dot.its.jpo.ode.mec.deposit.etx;

import java.math.BigDecimal;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import us.dot.its.jpo.ode.mec.deposit.etx.models.EtxClientSubType;
import us.dot.its.jpo.ode.mec.deposit.etx.models.EtxClientType;
import us.dot.its.jpo.ode.mec.deposit.etx.models.partner.EtxNetworkType;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;


/**
 * Configuration properties for ETX integration. Defines properties for API endpoints, MQTT
 * settings, and client configuration.
 */
@Configuration
@ConfigurationProperties(prefix = "etx")
@Data
public class EtxProperties {
  private boolean enabled;
  private String vendor;
  private EtxNetworkType networkType;
  private boolean cacheRegistration;
  private String certificatePath;
  private EtxClientType clientType;
  private EtxClientSubType clientSubType;
  private ClearTimProperties clearTim;
  private PartnerApiProperties partnerApi;
  private EtxMqttProperties mqtt;
  private EtxDepositors depositors;

  /**
   * Properties for the ETX Clear TIM configuration.
   */
  @Data
  public static class ClearTimProperties {
    private Boolean enabled;
    private Integer interval;
  }

  /**
   * Properties for the ETX Partner API configuration.
   */
  @Data
  public static class PartnerApiProperties {
    private String baseUri;
    private String username;
    private String password;
    private BigDecimal mecLatitude;
    private BigDecimal mecLongitude;
  }

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
  }
}
