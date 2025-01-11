package us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enumeration representing the different message formats for ETX (Edge Traffic Exchange) MQTT
 * messages.
 */
public enum EtxMqttMessageFormat {
  J2735("j2735"), J2735_GR("j2735_gr"), AVRO("avro"), JSON("json");

  private String value;

  /**
   * Constructs a message format with the specified value.
   *
   * @param value The string representation
   */
  EtxMqttMessageFormat(String value) {
    this.value = value;
  }

  /**
   * Gets the string value of this message format.
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

  /**
   * Creates a message format from its string value.
   *
   * @param value The string representation
   * @return The corresponding message format
   * @throws IllegalArgumentException if value is invalid
   */
  @JsonCreator
  public static EtxMqttMessageFormat fromValue(String value) {
    for (EtxMqttMessageFormat b : EtxMqttMessageFormat.values()) {
      if (b.value.equals(value)) {
        return b;
      }
    }
    throw new IllegalArgumentException("Unexpected value '" + value + "'");
  }
}
