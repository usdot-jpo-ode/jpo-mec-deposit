package us.dot.its.jpo.ode.mec.deposit.imp;

import java.math.BigDecimal;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import us.dot.its.jpo.ode.mec.deposit.imp.models.ImpClientSubType;
import us.dot.its.jpo.ode.mec.deposit.imp.models.ImpClientType;
import us.dot.its.jpo.ode.mec.deposit.imp.mqtt.ImpMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.imp.models.partner.ImpNetworkType;


/**
 * Configuration properties for the IMP (Infrastructure Message Processor) service.
 */
@Configuration
@ConfigurationProperties(prefix = "imp")
@Data
public class ImpProperties {
  private boolean enabled;
  private String vendor;
  private ImpNetworkType networkType;
  private boolean cacheRegistration;
  private String certificatePath;
  private ImpClientType clientType;
  private ImpClientSubType clientSubType;
  private ClearTimProperties clearTim;
  private PartnerApiProperties partnerApi;
  private ImpMqttProperties mqtt;
  private ImpDepositors depositors;

  /**
   * Properties for the IMP Clear TIM configuration.
   */
  @Data
  public static class ClearTimProperties {
    private Boolean enabled;
    private Integer interval;
  }

  /**
   * Properties for the IMP Partner API configuration.
   */
  @Data
  public static class PartnerApiProperties {
    private String baseUri;
    private String username;
    private String password;
    private BigDecimal mecLatitude;
    private BigDecimal mecLongitude;
  }

  @Data
  public static class ImpDepositors {
    private ImpDepositorProperties bsm;
    private ImpDepositorProperties spat;
    private ImpDepositorProperties tim;
    private ImpDepositorProperties map;
  }

  /**
   * Properties for IMP depositor configuration.
   */
  @Data
  public static class ImpDepositorProperties {
    private ImpMqttDepositorProperties mqtt;
    private ImpApiDepositorProperties api;
  }

  /**
   * Properties for the IMP Mqtt configuration.
   */
  @Data
  public static class ImpMqttDepositorProperties {
    private Boolean enabled;
    private String kafkaTopic;
  }

  /**
   * Properties for the IMP Api configuration.
   */
  @Data
  public static class ImpApiDepositorProperties {
    private Boolean enabled;
    private String kafkaTopic;
  }
}
