package us.dot.its.jpo.ode.mec.deposit.imp.models.partner;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;

import io.swagger.v3.oas.annotations.media.Schema;


/**
 * ClientConnectionPostRequest
 */

public class ClientConnectionPostRequest {

  private String deviceID;

  private BigDecimal lat;

  private BigDecimal _long;

  private ImpNetworkType networkType;

  public ClientConnectionPostRequest() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public ClientConnectionPostRequest(String deviceID, BigDecimal lat, BigDecimal _long,
      ImpNetworkType networkType) {
    this.deviceID = deviceID;
    this.lat = lat;
    this._long = _long;
    this.networkType = networkType;
  }

  public ClientConnectionPostRequest deviceID(String deviceID) {
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

  public ClientConnectionPostRequest lat(BigDecimal lat) {
    this.lat = lat;
    return this;
  }

  /**
   * Get lat
   * 
   * @return lat
   */
  @Schema(name = "lat", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("lat")
  public BigDecimal getLat() {
    return lat;
  }

  public void setLat(BigDecimal lat) {
    this.lat = lat;
  }

  public ClientConnectionPostRequest _long(BigDecimal _long) {
    this._long = _long;
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
    return _long;
  }

  public void setLong(BigDecimal _long) {
    this._long = _long;
  }

  public ClientConnectionPostRequest networkType(ImpNetworkType networkType) {
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
  public ImpNetworkType getNetworkType() {
    return networkType;
  }

  public void setNetworkType(ImpNetworkType networkType) {
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
    ClientConnectionPostRequest clientConnectionPostRequest = (ClientConnectionPostRequest) o;
    return Objects.equals(this.deviceID, clientConnectionPostRequest.deviceID)
        && Objects.equals(this.lat, clientConnectionPostRequest.lat)
        && Objects.equals(this._long, clientConnectionPostRequest._long)
        && Objects.equals(this.networkType, clientConnectionPostRequest.networkType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deviceID, lat, _long, networkType);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ClientConnectionPostRequest {\n");
    sb.append("    deviceID: ").append(toIndentedString(deviceID)).append("\n");
    sb.append("    lat: ").append(toIndentedString(lat)).append("\n");
    sb.append("    _long: ").append(toIndentedString(_long)).append("\n");
    sb.append("    networkType: ").append(toIndentedString(networkType)).append("\n");
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
