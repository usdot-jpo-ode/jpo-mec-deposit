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
 * ClientConnectionResponse
 */

public class ClientConnectionResponse {

  private String mqttURL;

  public ClientConnectionResponse() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public ClientConnectionResponse(String mqttURL) {
    this.mqttURL = mqttURL;
  }

  public ClientConnectionResponse mqttURL(String mqttURL) {
    this.mqttURL = mqttURL;
    return this;
  }

  /**
   * Get mqttURL
   * 
   * @return mqttURL
   */
  @Schema(name = "MqttURL", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("MqttURL")
  public String getMqttURL() {
    return mqttURL;
  }

  public void setMqttURL(String mqttURL) {
    this.mqttURL = mqttURL;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClientConnectionResponse clientConnectionResponse = (ClientConnectionResponse) o;
    return Objects.equals(this.mqttURL, clientConnectionResponse.mqttURL);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mqttURL);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ClientConnectionResponse {\n");
    sb.append("    mqttURL: ").append(toIndentedString(mqttURL)).append("\n");
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
