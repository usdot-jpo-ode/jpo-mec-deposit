package us.dot.its.jpo.ode.mec.deposit.imp.models.partner;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Gets or Sets ImpEnvironmentName
 */

public enum ImpEnvironmentName {

  PRD("prd"),

  STG("stg"),

  QA("qa"),

  DEV("dev");

  private String value;

  ImpEnvironmentName(String value) {
    this.value = value;
  }

  @JsonValue
  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return String.valueOf(value);
  }

  @JsonCreator
  public static ImpEnvironmentName fromValue(String value) {
    for (ImpEnvironmentName b : ImpEnvironmentName.values()) {
      if (b.value.equals(value)) {
        return b;
      }
    }
    throw new IllegalArgumentException("Unexpected value '" + value + "'");
  }
}
