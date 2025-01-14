package us.dot.its.jpo.ode.mec.deposit.etx.partner;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.NetworkType;
import java.math.BigDecimal;

@Configuration
@ConfigurationProperties(prefix = "mec-deposit.etx.partner-api")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EtxPartnerApiProperties {
  private String vendor;
  private String baseUri;
  private String username;
  private String password;
  private BigDecimal mecLatitude;
  private BigDecimal mecLongitude;
  private NetworkType networkType;
  private boolean cacheRegistration;
  private String certificatePath;
  private ClearTimProperties clearTim;

  /**
   * Properties for the ETX Clear TIM configuration.
   */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ClearTimProperties {
    private Boolean enabled;
    private Integer interval;
  }
}
