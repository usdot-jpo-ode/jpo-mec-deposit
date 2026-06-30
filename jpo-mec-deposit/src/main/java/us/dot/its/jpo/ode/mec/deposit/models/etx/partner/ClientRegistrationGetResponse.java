package us.dot.its.jpo.ode.mec.deposit.models.etx.partner;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientSubType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientType;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class ClientRegistrationGetResponse {

  @Schema(name = "DeviceID", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("DeviceID")
  private String deviceID;

  @Schema(name = "Certificate", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("Certificate")
  private CertificateResponse certificate;

  private EtxClientType clientType;

  private EtxClientSubType clientSubType;

  private String vendorId;
}
