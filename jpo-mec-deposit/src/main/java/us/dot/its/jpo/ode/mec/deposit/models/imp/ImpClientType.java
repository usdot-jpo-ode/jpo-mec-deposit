package us.dot.its.jpo.ode.mec.deposit.models.imp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enumeration representing the different types of IMP (Infrastructure Message Processor) clients.
 */
public enum ImpClientType {
  // "Vehicle""VulnerableRoadUser""TrafficLightController""InfrastructureSensor""OnboardSensor""Software"
  VEHICLE("Vehicle"), VULNERABLE_ROAD_USER("VulnerableRoadUser"), TRAFFIC_LIGHT_CONTROLLER(
      "TrafficLightController"), INFRASTRUCTURE_SENSOR(
          "InfrastructureSensor"), ONBOARD_SENSOR("OnboardSensor"), SOFTWARE("Software");

  /** String value of the client type. */
  private String value;

  /**
   * Constructs a client type with the specified value.
   *
   * @param value The string representation
   */
  ImpClientType(String value) {
    this.value = value;
  }

  /**
   * Gets the string value of this client type.
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
   * Creates a client type from its string value.
   *
   * @param value The string representation
   * @return The corresponding client type
   * @throws IllegalArgumentException if value is invalid
   */
  @JsonCreator
  public static ImpClientType fromValue(String value) {
    for (ImpClientType b : ImpClientType.values()) {
      if (b.value.equals(value)) {
        return b;
      }
    }
    throw new IllegalArgumentException("Unexpected value '" + value + "'");
  }
}
