package us.dot.its.jpo.ode.mec.deposit.models.imp.partner;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.*;

/**
 * CertificateResponse
 */

public class CertificateResponse {

  private String certPem;

  private String keyPem;

  private String caPem;

  public CertificateResponse() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public CertificateResponse(String certPem, String keyPem, String caPem) {
    this.certPem = certPem;
    this.keyPem = keyPem;
    this.caPem = caPem;
  }

  public CertificateResponse certPem(String certPem) {
    this.certPem = certPem;
    return this;
  }

  /**
   * Get certPem
   * 
   * @return certPem
   */
  @Schema(name = "cert.pem", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("cert.pem")
  public String getCertPem() {
    return certPem;
  }

  public void setCertPem(String certPem) {
    this.certPem = certPem;
  }

  public CertificateResponse keyPem(String keyPem) {
    this.keyPem = keyPem;
    return this;
  }

  /**
   * Get keyPem
   * 
   * @return keyPem
   */
  @Schema(name = "key.pem", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("key.pem")
  public String getKeyPem() {
    return keyPem;
  }

  public void setKeyPem(String keyPem) {
    this.keyPem = keyPem;
  }

  public CertificateResponse caPem(String caPem) {
    this.caPem = caPem;
    return this;
  }

  /**
   * Get caPem
   * 
   * @return caPem
   */
  @Schema(name = "ca.pem", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("ca.pem")
  public String getCaPem() {
    return caPem;
  }

  public void setCaPem(String caPem) {
    this.caPem = caPem;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CertificateResponse certificateResponse = (CertificateResponse) o;
    return Objects.equals(this.certPem, certificateResponse.certPem)
        && Objects.equals(this.keyPem, certificateResponse.keyPem)
        && Objects.equals(this.caPem, certificateResponse.caPem);
  }

  @Override
  public int hashCode() {
    return Objects.hash(certPem, keyPem, caPem);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class CertificateResponse {\n");
    sb.append("    certPem: ").append(toIndentedString(certPem)).append("\n");
    sb.append("    keyPem: ").append(toIndentedString(keyPem)).append("\n");
    sb.append("    caPem: ").append(toIndentedString(caPem)).append("\n");
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
