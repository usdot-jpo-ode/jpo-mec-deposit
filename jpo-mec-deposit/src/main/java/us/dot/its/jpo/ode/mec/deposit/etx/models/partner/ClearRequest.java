package us.dot.its.jpo.ode.mec.deposit.etx.models.partner;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a request to clear the deployed configuration.
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class ClearRequest {
  @JsonProperty("clear_tim_only")
  private boolean clearTimOnly;
}
