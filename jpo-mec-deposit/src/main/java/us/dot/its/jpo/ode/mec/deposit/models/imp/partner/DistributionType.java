package us.dot.its.jpo.ode.mec.deposit.models.imp.partner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttNamespace;

/**
 * Enumeration representing the different distribution types for IMP (Infrastructure Message
 * Processor) messages.
 */
public enum DistributionType {
  TARGETED("targeted"), BROADCAST("broadcast");

  private final String value;

  DistributionType(String value) {
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

}
