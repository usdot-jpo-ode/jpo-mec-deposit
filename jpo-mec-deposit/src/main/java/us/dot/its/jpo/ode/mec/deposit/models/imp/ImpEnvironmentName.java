package us.dot.its.jpo.ode.mec.deposit.models.imp;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonValue;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.*;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

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
