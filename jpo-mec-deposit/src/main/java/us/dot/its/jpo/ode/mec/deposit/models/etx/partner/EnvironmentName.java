package us.dot.its.jpo.ode.mec.deposit.models.etx.partner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Gets or Sets EtxEnvironmentName
 */

public enum EnvironmentName {

  PRD("prd"),

  STG("stg"),

  QA("qa"),

  DEV("dev");

  private String value;

  EnvironmentName(String value) {
    this.value = value;
  }

  @JsonCreator
  public static EnvironmentName fromValue(String value) {
    for (EnvironmentName b : EnvironmentName.values()) {
      if (b.value.equals(value)) {
        return b;
      }
    }
    throw new IllegalArgumentException("Unexpected value '" + value + "'");
  }

  @JsonValue
  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return String.valueOf(value);
  }
}
