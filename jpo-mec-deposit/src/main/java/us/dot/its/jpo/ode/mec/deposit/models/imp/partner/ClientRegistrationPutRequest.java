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
 * ClientRegistrationPutRequest
 */

public class ClientRegistrationPutRequest {

  private String deviceID;

  public ClientRegistrationPutRequest() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public ClientRegistrationPutRequest(String deviceID) {
    this.deviceID = deviceID;
  }

  public ClientRegistrationPutRequest deviceID(String deviceID) {
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClientRegistrationPutRequest clientRegistrationPutRequest = (ClientRegistrationPutRequest) o;
    return Objects.equals(this.deviceID, clientRegistrationPutRequest.deviceID);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deviceID);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ClientRegistrationPutRequest {\n");
    sb.append("    deviceID: ").append(toIndentedString(deviceID)).append("\n");
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
