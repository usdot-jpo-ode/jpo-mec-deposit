package us.dot.its.jpo.ode.mec.deposit.models.etx.partner;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;


import io.swagger.v3.oas.annotations.media.Schema;


/**
 * ClientRegistrationResponse
 */

public class ClientRegistrationResponse {

  private String deviceID;

  private CertificateResponse certificate;

  public ClientRegistrationResponse() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public ClientRegistrationResponse(String deviceID, CertificateResponse certificate) {
    this.deviceID = deviceID;
    this.certificate = certificate;
  }

  public ClientRegistrationResponse deviceID(String deviceID) {
    this.deviceID = deviceID;
    return this;
  }

  /**
   * Get deviceID
   * 
   * @return deviceID
   */
  @Schema(name = "DeviceID", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("DeviceID")
  public String getDeviceID() {
    return deviceID;
  }

  public void setDeviceID(String deviceID) {
    this.deviceID = deviceID;
  }

  public ClientRegistrationResponse certificate(CertificateResponse certificate) {
    this.certificate = certificate;
    return this;
  }

  /**
   * Get certificate
   * 
   * @return certificate
   */
  @Schema(name = "Certificate", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("Certificate")
  public CertificateResponse getCertificate() {
    return certificate;
  }

  public void setCertificate(CertificateResponse certificate) {
    this.certificate = certificate;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClientRegistrationResponse clientRegistrationResponse = (ClientRegistrationResponse) o;
    return Objects.equals(this.deviceID, clientRegistrationResponse.deviceID)
        && Objects.equals(this.certificate, clientRegistrationResponse.certificate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deviceID, certificate);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ClientRegistrationResponse {\n");
    sb.append("    deviceID: ").append(toIndentedString(deviceID)).append("\n");
    sb.append("    certificate: ").append(toIndentedString(certificate)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}
