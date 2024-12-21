package us.dot.its.jpo.ode.mec.deposit.imp;

import java.math.BigDecimal;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import us.dot.its.jpo.ode.mec.deposit.imp.mqtt.ImpMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.imp.ImpClientSubType;
import us.dot.its.jpo.ode.mec.deposit.models.imp.ImpClientType;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.ImpNetworkType;


/**
 * Configuration properties for the IMP (Infrastructure Message Processor) service.
 */
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

  /**
   * Properties for the IMP Partner API configuration.
   */
  @Data
  public static class PartnerApiProperties {
    private String baseUri;
    private String user;
    private String pass;
    private DepositorProperties depositors;
    private BigDecimal mecLatitude;
    private BigDecimal mecLongitude;
  }

  /**
   * Properties for IMP depositor configuration.
   */
  @Data
  public static class DepositorProperties {
    private MapProperties map;
    private TimProperties tim;
  }

  /**
   * Properties for MAP message configuration.
   */
  @Data
  public static class MapProperties {
    private String sourceKafkaTopic;
    private String distributionType;
  }

  /**
   * Properties for TIM message configuration.
   */
  @Data
  public static class TimProperties {
    private String sourceKafkaTopic;
    private String distributionType;
  }
}
