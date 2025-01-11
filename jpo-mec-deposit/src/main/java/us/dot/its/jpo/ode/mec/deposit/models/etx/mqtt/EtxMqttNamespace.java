package us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enumeration representing the different namespaces for ETX (Infrastructure Message Processor) MQTT
 * messages.
 */
public enum EtxMqttNamespace {
  GEO_RELEVANCE("GeoRelevance"), REGIONAL("Regional"), PRIVATE("Private");

  private final String value;

  EtxMqttNamespace(String value) {
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
   * Creates a namespace from its string value.
   *
   * @param value The string representation
   * @return The corresponding namespace
   * @throws IllegalArgumentException if value is invalid
   */
  @JsonCreator
  public static EtxMqttNamespace fromValue(String value) {
    for (EtxMqttNamespace b : EtxMqttNamespace.values()) {
      if (b.value.equals(value)) {
        return b;
      }
    }
    throw new IllegalArgumentException("Unexpected value '" + value + "'");
  }

}
