package us.dot.its.jpo.ode.mec.deposit.models.etx.partner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enumeration representing the different distribution types for ETX (Infrastructure Message
 * Processor) messages.
 */
public enum DistributionType {
  TARGETED("Targeted"), BROADCAST("Broadcast");

  private final String value;

  DistributionType(String value) {
    this.value = value;
  }

  /**
   * Creates a distribution type from its string value.
   *
   * @param value The string representation
   * @return The corresponding distribution type
   * @throws IllegalArgumentException if value is invalid
   */
  @JsonCreator
  public static DistributionType fromValue(String value) {
    for (DistributionType b : DistributionType.values()) {
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
