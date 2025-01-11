package us.dot.its.jpo.ode.mec.deposit.models.etx.partner;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.v3.oas.annotations.media.Schema;


/**
 * AuthRefreshTokenRequest
 */

public class AuthRefreshTokenRequest {

  private String refreshToken;

  public AuthRefreshTokenRequest() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public AuthRefreshTokenRequest(String refreshToken) {
    this.refreshToken = refreshToken;
  }

  public AuthRefreshTokenRequest refreshToken(String refreshToken) {
    this.refreshToken = refreshToken;
    return this;
  }

  /**
   * Get refreshToken
   * 
   * @return refreshToken
   */
  @Schema(name = "refresh_token", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("refresh_token")
  public String getRefreshToken() {
    return refreshToken;
  }

  public void setRefreshToken(String refreshToken) {
    this.refreshToken = refreshToken;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuthRefreshTokenRequest authRefreshTokenRequest = (AuthRefreshTokenRequest) o;
    return Objects.equals(this.refreshToken, authRefreshTokenRequest.refreshToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(refreshToken);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class AuthRefreshTokenRequest {\n");
    sb.append("    refreshToken: ").append(toIndentedString(refreshToken)).append("\n");
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
