package us.dot.its.jpo.ode.mec.deposit.etx.models.partner;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a request to deposit an ASN.1 encoded message for distribution to ETX partners.
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class DepositRequest {
  @JsonProperty("asn1_hex")
  private String asn1Hex;
  @JsonProperty("distribution_type")
  private DistributionType distributionType;
}
