package us.dot.its.jpo.ode.mec.deposit.imp.models.partner;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.v3.oas.annotations.media.Schema;


/**
 * AuthTokenRequest
 */

public class AuthTokenRequest {

  private String username;

  private String password;

  public AuthTokenRequest() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public AuthTokenRequest(String username, String password) {
    this.username = username;
    this.password = password;
  }

  public AuthTokenRequest username(String username) {
    this.username = username;
    return this;
  }

  /**
   * Get username
   * 
   * @return username
   */
  @Schema(name = "username", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("username")
  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public AuthTokenRequest password(String password) {
    this.password = password;
    return this;
  }

  /**
   * Get password
   * 
   * @return password
   */
  @Schema(name = "password", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("password")
  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuthTokenRequest authTokenRequest = (AuthTokenRequest) o;
    return Objects.equals(this.username, authTokenRequest.username)
        && Objects.equals(this.password, authTokenRequest.password);
  }

  @Override
  public int hashCode() {
    return Objects.hash(username, password);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class AuthTokenRequest {\n");
    sb.append("    username: ").append(toIndentedString(username)).append("\n");
    sb.append("    password: ").append(toIndentedString(password)).append("\n");
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
