package us.dot.its.jpo.ode.mec.deposit.imp.models.partner;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;


import io.swagger.v3.oas.annotations.media.Schema;


/**
 * ClientErrorResponse
 */

public class ClientErrorResponse {

  private String error;

  private String description;

  public ClientErrorResponse() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public ClientErrorResponse(String error, String description) {
    this.error = error;
    this.description = description;
  }

  public ClientErrorResponse error(String error) {
    this.error = error;
    return this;
  }

  /**
   * Get error
   * 
   * @return error
   */
  @Schema(name = "error", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("error")
  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }

  public ClientErrorResponse description(String description) {
    this.description = description;
    return this;
  }

  /**
   * Get description
   * 
   * @return description
   */
  @Schema(name = "description", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("description")
  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClientErrorResponse clientErrorResponse = (ClientErrorResponse) o;
    return Objects.equals(this.error, clientErrorResponse.error)
        && Objects.equals(this.description, clientErrorResponse.description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(error, description);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ClientErrorResponse {\n");
    sb.append("    error: ").append(toIndentedString(error)).append("\n");
    sb.append("    description: ").append(toIndentedString(description)).append("\n");
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
