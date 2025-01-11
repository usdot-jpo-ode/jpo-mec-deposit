package us.dot.its.jpo.ode.mec.deposit.models.etx.partner;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Gets or Sets ImpEnvironmentName
 */

public enum EtxEnvironmentName {

  PRD("prd"),

  STG("stg"),

  QA("qa"),

  DEV("dev");

  private String value;

  EtxEnvironmentName(String value) {
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
  public static EtxEnvironmentName fromValue(String value) {
    for (EtxEnvironmentName b : EtxEnvironmentName.values()) {
      if (b.value.equals(value)) {
        return b;
      }
    }
    throw new IllegalArgumentException("Unexpected value '" + value + "'");
  }
}
