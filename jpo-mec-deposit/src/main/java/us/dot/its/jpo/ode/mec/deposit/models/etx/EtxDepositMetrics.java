package us.dot.its.jpo.ode.mec.deposit.models.etx;

import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.DistributionType;

/**
 * Abstract class representing metrics for ETX deposits. Contains common fields for tracking deposit
 * operations including timing, status, and routing information.
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class EtxDepositMetrics {
  private EtxDepositorType depositorType;
  private EtxMessageType messageType;
  private long odeReceivedAt;
  private long mecDepositedAt;
  private long latencyMs;
  private boolean success;
  private String errorMessage;
  private Set<String> topics;
  private DistributionType distributionType;
}
