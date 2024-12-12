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
 * AuthToken
 */

public class AuthToken {

  private String accessToken;

  private Integer expiresIn;

  private Integer refreshExpiresIn;

  private String refreshToken;

  private String tokenType;

  private String idToken;

  private Integer notBeforePolicy;

  private String sessionState;

  private String scope;

  public AuthToken() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public AuthToken(String accessToken, Integer expiresIn, Integer refreshExpiresIn, String refreshToken,
      String tokenType, String idToken, Integer notBeforePolicy, String sessionState, String scope) {
    this.accessToken = accessToken;
    this.expiresIn = expiresIn;
    this.refreshExpiresIn = refreshExpiresIn;
    this.refreshToken = refreshToken;
    this.tokenType = tokenType;
    this.idToken = idToken;
    this.notBeforePolicy = notBeforePolicy;
    this.sessionState = sessionState;
    this.scope = scope;
  }

  public AuthToken accessToken(String accessToken) {
    this.accessToken = accessToken;
    return this;
  }

  /**
   * Get accessToken
   * 
   * @return accessToken
   */
  @Schema(name = "access_token", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("access_token")
  public String getAccessToken() {
    return accessToken;
  }

  public void setAccessToken(String accessToken) {
    this.accessToken = accessToken;
  }

  public AuthToken expiresIn(Integer expiresIn) {
    this.expiresIn = expiresIn;
    return this;
  }

  /**
   * Get expiresIn
   * 
   * @return expiresIn
   */
  @Schema(name = "expires_in", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("expires_in")
  public Integer getExpiresIn() {
    return expiresIn;
  }

  public void setExpiresIn(Integer expiresIn) {
    this.expiresIn = expiresIn;
  }

  public AuthToken refreshExpiresIn(Integer refreshExpiresIn) {
    this.refreshExpiresIn = refreshExpiresIn;
    return this;
  }

  /**
   * Get refreshExpiresIn
   * 
   * @return refreshExpiresIn
   */
  @Schema(name = "refresh_expires_in", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("refresh_expires_in")
  public Integer getRefreshExpiresIn() {
    return refreshExpiresIn;
  }

  public void setRefreshExpiresIn(Integer refreshExpiresIn) {
    this.refreshExpiresIn = refreshExpiresIn;
  }

  public AuthToken refreshToken(String refreshToken) {
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

  public AuthToken tokenType(String tokenType) {
    this.tokenType = tokenType;
    return this;
  }

  /**
   * Get tokenType
   * 
   * @return tokenType
   */
  @Schema(name = "token_type", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("token_type")
  public String getTokenType() {
    return tokenType;
  }

  public void setTokenType(String tokenType) {
    this.tokenType = tokenType;
  }

  public AuthToken idToken(String idToken) {
    this.idToken = idToken;
    return this;
  }

  /**
   * Get idToken
   * 
   * @return idToken
   */
  @Schema(name = "id_token", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("id_token")
  public String getIdToken() {
    return idToken;
  }

  public void setIdToken(String idToken) {
    this.idToken = idToken;
  }

  public AuthToken notBeforePolicy(Integer notBeforePolicy) {
    this.notBeforePolicy = notBeforePolicy;
    return this;
  }

  /**
   * Get notBeforePolicy
   * 
   * @return notBeforePolicy
   */
  @Schema(name = "not-before-policy", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("not-before-policy")
  public Integer getNotBeforePolicy() {
    return notBeforePolicy;
  }

  public void setNotBeforePolicy(Integer notBeforePolicy) {
    this.notBeforePolicy = notBeforePolicy;
  }

  public AuthToken sessionState(String sessionState) {
    this.sessionState = sessionState;
    return this;
  }

  /**
   * Get sessionState
   * 
   * @return sessionState
   */
  @Schema(name = "session_state", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("session_state")
  public String getSessionState() {
    return sessionState;
  }

  public void setSessionState(String sessionState) {
    this.sessionState = sessionState;
  }

  public AuthToken scope(String scope) {
    this.scope = scope;
    return this;
  }

  /**
   * Get scope
   * 
   * @return scope
   */
  @Schema(name = "scope", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("scope")
  public String getScope() {
    return scope;
  }

  public void setScope(String scope) {
    this.scope = scope;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuthToken authToken = (AuthToken) o;
    return Objects.equals(this.accessToken, authToken.accessToken)
        && Objects.equals(this.expiresIn, authToken.expiresIn)
        && Objects.equals(this.refreshExpiresIn, authToken.refreshExpiresIn)
        && Objects.equals(this.refreshToken, authToken.refreshToken)
        && Objects.equals(this.tokenType, authToken.tokenType) && Objects.equals(this.idToken, authToken.idToken)
        && Objects.equals(this.notBeforePolicy, authToken.notBeforePolicy)
        && Objects.equals(this.sessionState, authToken.sessionState) && Objects.equals(this.scope, authToken.scope);
  }

  @Override
  public int hashCode() {
    return Objects.hash(accessToken, expiresIn, refreshExpiresIn, refreshToken, tokenType, idToken, notBeforePolicy,
        sessionState, scope);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class AuthToken {\n");
    sb.append("    accessToken: ").append(toIndentedString(accessToken)).append("\n");
    sb.append("    expiresIn: ").append(toIndentedString(expiresIn)).append("\n");
    sb.append("    refreshExpiresIn: ").append(toIndentedString(refreshExpiresIn)).append("\n");
    sb.append("    refreshToken: ").append(toIndentedString(refreshToken)).append("\n");
    sb.append("    tokenType: ").append(toIndentedString(tokenType)).append("\n");
    sb.append("    idToken: ").append(toIndentedString(idToken)).append("\n");
    sb.append("    notBeforePolicy: ").append(toIndentedString(notBeforePolicy)).append("\n");
    sb.append("    sessionState: ").append(toIndentedString(sessionState)).append("\n");
    sb.append("    scope: ").append(toIndentedString(scope)).append("\n");
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
