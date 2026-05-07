package us.dot.its.jpo.ode.mec.deposit.models.etx;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

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
  /**
   * MQTT broker targets that successfully received this message (e.g. ETX, NMI). Populated for
   * multi-broker MQTT depositors; omitted when not applicable.
   */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private Set<String> mqttBrokerTargets;
  private String asn1Hex;
}
