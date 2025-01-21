package us.dot.its.jpo.ode.mec.deposit.models.etx;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enumeration representing the different message types for ETX (Edge Traffic Exchange) messages.
 */
public enum EtxMessageType {
  BSM("BSM"), PSM("PSM"), RSA("RSA"), TIM("TIM"), MAP("MAP"), SPAT("SPAT");

  private String value;

  /**
   * Constructs a message type with the specified value.
   *
   * @param value The string representation
   */
  EtxMessageType(String value) {
    this.value = value;
  }

  /**
   * Creates a message type from its string value.
   *
   * @param value The string representation
   * @return The corresponding message type
   * @throws IllegalArgumentException if value is invalid
   */
  @JsonCreator
  public static EtxMessageType fromValue(String value) {
    for (EtxMessageType b : EtxMessageType.values()) {
      if (b.value.equals(value)) {
        return b;
      }
    }
    throw new IllegalArgumentException("Unexpected value '" + value + "'");
  }

  /**
   * Gets the string value of this message type.
   *
   * @return The string representation
   */
  @JsonValue
  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return String.valueOf(value);
  }
}
