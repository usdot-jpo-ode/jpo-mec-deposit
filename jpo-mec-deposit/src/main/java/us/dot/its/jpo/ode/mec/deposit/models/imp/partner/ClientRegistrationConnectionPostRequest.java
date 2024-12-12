package us.dot.its.jpo.ode.mec.deposit.models.imp.partner;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.math.BigDecimal;

import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;

import io.swagger.v3.oas.annotations.media.Schema;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.NetworkType;

import java.util.*;

/**
 * ClientRegistrationConnectionPostRequest
 */

public class ClientRegistrationConnectionPostRequest {

  private String clientType;

  private String clientSubtype;

  private BigDecimal deviceFixedLocationLat;

  private BigDecimal deviceFixedLocationLong;

  private NetworkType networkType;

  public ClientRegistrationConnectionPostRequest() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public ClientRegistrationConnectionPostRequest(String clientType, String clientSubtype, BigDecimal lat,
      BigDecimal _long, NetworkType networkType) {
    this.clientType = clientType;
    this.clientSubtype = clientSubtype;
    this.deviceFixedLocationLat = lat;
    this.deviceFixedLocationLong = _long;
    this.networkType = networkType;
  }

  public ClientRegistrationConnectionPostRequest clientType(String clientType) {
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

  public ClientRegistrationConnectionPostRequest clientSubtype(String clientSubtype) {
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

  public ClientRegistrationConnectionPostRequest lat(BigDecimal lat) {
    this.deviceFixedLocationLat = lat;
    return this;
  }

  /**
   * Get lat
   * 
   * @return lat
   */
  @Schema(name = "lat", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("lat")
  public BigDecimal getDeviceFixedLocationLat() {
    return deviceFixedLocationLat;
  }

  public void setDeviceFixedLocationLat(BigDecimal lat) {
    this.deviceFixedLocationLat = lat;
  }

  public ClientRegistrationConnectionPostRequest _long(BigDecimal _long) {
    this.deviceFixedLocationLong = _long;
    return this;
  }

  /**
   * Get _long
   * 
   * @return _long
   */
  @Schema(name = "long", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("long")
  public BigDecimal getLong() {
    return deviceFixedLocationLong;
  }

  public void setLong(BigDecimal _long) {
    this.deviceFixedLocationLong = _long;
  }

  public ClientRegistrationConnectionPostRequest networkType(NetworkType networkType) {
    this.networkType = networkType;
    return this;
  }

  /**
   * Get networkType
   * 
   * @return networkType
   */
  @Schema(name = "NetworkType", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("NetworkType")
  public NetworkType getNetworkType() {
    return networkType;
  }

  public void setNetworkType(NetworkType networkType) {
    this.networkType = networkType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClientRegistrationConnectionPostRequest clientRegistrationConnectionPostRequest = (ClientRegistrationConnectionPostRequest) o;
    return Objects.equals(this.clientType, clientRegistrationConnectionPostRequest.clientType)
        && Objects.equals(this.clientSubtype, clientRegistrationConnectionPostRequest.clientSubtype)
        && Objects.equals(this.deviceFixedLocationLat, clientRegistrationConnectionPostRequest.deviceFixedLocationLat)
        && Objects.equals(this.deviceFixedLocationLong, clientRegistrationConnectionPostRequest.deviceFixedLocationLong)
        && Objects.equals(this.networkType, clientRegistrationConnectionPostRequest.networkType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clientType, clientSubtype, deviceFixedLocationLat, deviceFixedLocationLong, networkType);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ClientRegistrationConnectionPostRequest {\n");
    sb.append("    clientType: ").append(toIndentedString(clientType)).append("\n");
    sb.append("    clientSubtype: ").append(toIndentedString(clientSubtype)).append("\n");
    sb.append("    lat: ").append(toIndentedString(deviceFixedLocationLat)).append("\n");
    sb.append("    _long: ").append(toIndentedString(deviceFixedLocationLong)).append("\n");
    sb.append("    networkType: ").append(toIndentedString(networkType)).append("\n");
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
