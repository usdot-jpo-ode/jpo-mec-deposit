package us.dot.its.jpo.ode.mec.deposit.models.imp;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.*;

/**
 * ClientRegistrationPostRequest
 */

public class ClientRegistrationPostRequest {

  private String clientType;

  private String clientSubtype;

  public ClientRegistrationPostRequest() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public ClientRegistrationPostRequest(String clientType, String clientSubtype) {
    this.clientType = clientType;
    this.clientSubtype = clientSubtype;
  }

  public ClientRegistrationPostRequest clientType(String clientType) {
    this.clientType = clientType;
    return this;
  }

  /**
   * Get clientType
   * 
   * @return clientType
   */
  @Schema(name = "ClientType", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("ClientType")
  public String getClientType() {
    return clientType;
  }

  public void setClientType(String clientType) {
    this.clientType = clientType;
  }

  public ClientRegistrationPostRequest clientSubtype(String clientSubtype) {
    this.clientSubtype = clientSubtype;
    return this;
  }

  /**
   * Get clientSubtype
   * 
   * @return clientSubtype
   */
  @Schema(name = "ClientSubtype", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("ClientSubtype")
  public String getClientSubtype() {
    return clientSubtype;
  }

  public void setClientSubtype(String clientSubtype) {
    this.clientSubtype = clientSubtype;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClientRegistrationPostRequest clientRegistrationPostRequest = (ClientRegistrationPostRequest) o;
    return Objects.equals(this.clientType, clientRegistrationPostRequest.clientType)
        && Objects.equals(this.clientSubtype, clientRegistrationPostRequest.clientSubtype);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clientType, clientSubtype);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ClientRegistrationPostRequest {\n");
    sb.append("    clientType: ").append(toIndentedString(clientType)).append("\n");
    sb.append("    clientSubtype: ").append(toIndentedString(clientSubtype)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}
