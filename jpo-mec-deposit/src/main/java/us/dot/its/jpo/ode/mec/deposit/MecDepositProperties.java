package us.dot.its.jpo.ode.mec.deposit;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;


/**
 * Configuration properties for the MEC Deposit service.
 */
@Configuration
@ConfigurationProperties(prefix = "mec-deposit")
@Data
public class MecDepositProperties {
  private MecDepositMetrics metrics;
  private EtxProperties etx;

  /**
   * Properties for the MEC Deposit metrics.
   */
  @Data
  public static class MecDepositMetrics {
    private boolean enabled;
    private String kafkaTopic;
  }
}
