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
 * ClientCompleteResponse
 */

public class ClientCompleteResponse {

  private ClientRegistrationResponse registration;

  private ClientConnectionResponse connection;

  public ClientCompleteResponse() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public ClientCompleteResponse(ClientRegistrationResponse registration,
      ClientConnectionResponse connection) {
    this.registration = registration;
    this.connection = connection;
  }

  public ClientCompleteResponse registration(ClientRegistrationResponse registration) {
    this.registration = registration;
    return this;
  }

  /**
   * Get registration
   * 
   * @return registration
   */
  @Schema(name = "registration", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("registration")
  public ClientRegistrationResponse getRegistration() {
    return registration;
  }

  public void setRegistration(ClientRegistrationResponse registration) {
    this.registration = registration;
  }

  public ClientCompleteResponse connection(ClientConnectionResponse connection) {
    this.connection = connection;
    return this;
  }

  /**
   * Get connection
   * 
   * @return connection
   */
  @Schema(name = "connection", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("connection")
  public ClientConnectionResponse getConnection() {
    return connection;
  }

  public void setConnection(ClientConnectionResponse connection) {
    this.connection = connection;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClientCompleteResponse clientCompleteResponse = (ClientCompleteResponse) o;
    return Objects.equals(this.registration, clientCompleteResponse.registration)
        && Objects.equals(this.connection, clientCompleteResponse.connection);
  }

  @Override
  public int hashCode() {
    return Objects.hash(registration, connection);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ClientCompleteResponse {\n");
    sb.append("    registration: ").append(toIndentedString(registration)).append("\n");
    sb.append("    connection: ").append(toIndentedString(connection)).append("\n");
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
