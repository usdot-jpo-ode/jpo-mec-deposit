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
 * AuthUserInfo
 */

public class AuthUserInfo {

  private String sub;

  private Boolean emailVerified;

  private String name;

  private String preferredUsername;

  private String givenName;

  private String familyName;

  private String email;

  public AuthUserInfo() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public AuthUserInfo(String sub, Boolean emailVerified, String name, String preferredUsername,
      String givenName, String familyName, String email) {
    this.sub = sub;
    this.emailVerified = emailVerified;
    this.name = name;
    this.preferredUsername = preferredUsername;
    this.givenName = givenName;
    this.familyName = familyName;
    this.email = email;
  }

  public AuthUserInfo sub(String sub) {
    this.sub = sub;
    return this;
  }

  /**
   * Get sub
   * 
   * @return sub
   */
  @Schema(name = "sub", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("sub")
  public String getSub() {
    return sub;
  }

  public void setSub(String sub) {
    this.sub = sub;
  }

  public AuthUserInfo emailVerified(Boolean emailVerified) {
    this.emailVerified = emailVerified;
    return this;
  }

  /**
   * Get emailVerified
   * 
   * @return emailVerified
   */
  @Schema(name = "email_verified", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("email_verified")
  public Boolean getEmailVerified() {
    return emailVerified;
  }

  public void setEmailVerified(Boolean emailVerified) {
    this.emailVerified = emailVerified;
  }

  public AuthUserInfo name(String name) {
    this.name = name;
    return this;
  }

  /**
   * Get name
   * 
   * @return name
   */
  @Schema(name = "name", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public AuthUserInfo preferredUsername(String preferredUsername) {
    this.preferredUsername = preferredUsername;
    return this;
  }

  /**
   * Get preferredUsername
   * 
   * @return preferredUsername
   */
  @Schema(name = "preferred_username", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("preferred_username")
  public String getPreferredUsername() {
    return preferredUsername;
  }

  public void setPreferredUsername(String preferredUsername) {
    this.preferredUsername = preferredUsername;
  }

  public AuthUserInfo givenName(String givenName) {
    this.givenName = givenName;
    return this;
  }

  /**
   * Get givenName
   * 
   * @return givenName
   */
  @Schema(name = "given_name", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("given_name")
  public String getGivenName() {
    return givenName;
  }

  public void setGivenName(String givenName) {
    this.givenName = givenName;
  }

  public AuthUserInfo familyName(String familyName) {
    this.familyName = familyName;
    return this;
  }

  /**
   * Get familyName
   * 
   * @return familyName
   */
  @Schema(name = "family_name", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("family_name")
  public String getFamilyName() {
    return familyName;
  }

  public void setFamilyName(String familyName) {
    this.familyName = familyName;
  }

  public AuthUserInfo email(String email) {
    this.email = email;
    return this;
  }

  /**
   * Get email
   * 
   * @return email
   */
  @Schema(name = "email", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("email")
  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuthUserInfo authUserInfo = (AuthUserInfo) o;
    return Objects.equals(this.sub, authUserInfo.sub)
        && Objects.equals(this.emailVerified, authUserInfo.emailVerified)
        && Objects.equals(this.name, authUserInfo.name)
        && Objects.equals(this.preferredUsername, authUserInfo.preferredUsername)
        && Objects.equals(this.givenName, authUserInfo.givenName)
        && Objects.equals(this.familyName, authUserInfo.familyName)
        && Objects.equals(this.email, authUserInfo.email);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sub, emailVerified, name, preferredUsername, givenName, familyName, email);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class AuthUserInfo {\n");
    sb.append("    sub: ").append(toIndentedString(sub)).append("\n");
    sb.append("    emailVerified: ").append(toIndentedString(emailVerified)).append("\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    preferredUsername: ").append(toIndentedString(preferredUsername)).append("\n");
    sb.append("    givenName: ").append(toIndentedString(givenName)).append("\n");
    sb.append("    familyName: ").append(toIndentedString(familyName)).append("\n");
    sb.append("    email: ").append(toIndentedString(email)).append("\n");
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
