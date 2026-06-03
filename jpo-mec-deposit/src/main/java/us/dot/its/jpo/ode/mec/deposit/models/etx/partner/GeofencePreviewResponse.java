package us.dot.its.jpo.ode.mec.deposit.models.etx.partner;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response from the ETX Partner API geofence preview endpoint.
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class GeofencePreviewResponse {
  @JsonProperty("geofence_id")
  private String geofenceId;

  private List<String> geohashes;

  @JsonProperty("geohash_count")
  private Integer geohashCount;
}
