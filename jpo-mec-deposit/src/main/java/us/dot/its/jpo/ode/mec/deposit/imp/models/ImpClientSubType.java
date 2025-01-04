package us.dot.its.jpo.ode.mec.deposit.imp.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enumeration representing the subtypes of IMP clients.
 */
public enum ImpClientSubType {
  PASSENGER_CAR("PassengerCar"), TRUCK("Truck"), BUS("Bus"), EMERGENCY_VEHICLE(
      "EmergencyVehicle"), SCHOOL_BUS("SchoolBus"), MAINTENANCE_VEHICLE(
          "MaintenanceVehicle"), PEDESTRIAN("Pedestrian"), BICYCLE("Bicycle"), SCOOTER(
              "Scooter"), MOTORCYCLE("Motorcycle"), ROAD_SIDE_UNIT("RoadSideUnit"), CAMERA(
                  "Camera"), LIDAR("Lidar"), RADAR("Radar"), INDUCTIVE_LOOP(
                      "InductiveLoop"), MAGNETIC_SENSOR("MagneticSensor"), PLATFORM(
                          "Platform"), APPLICATION("Application"), NA("NA");

  /** String value of the client subtype. */
  private String value;

  /**
   * Constructs a client subtype with the specified value.
   *
   * @param value The string representation
   */
  ImpClientSubType(String value) {
    this.value = value;
  }

  /**
   * Gets the string value of this client subtype.
   *
   * @return The string representation
   */
  @JsonValue
  public String getValue() {
    return value;
  }

  /**
   * Creates a client subtype from its string value.
   *
   * @param value The string representation
   * @return The corresponding client subtype
   * @throws IllegalArgumentException if value is invalid
   */
  @JsonCreator
  public static ImpClientSubType fromValue(String value) {
    for (ImpClientSubType b : ImpClientSubType.values()) {
      if (b.value.equals(value)) {
        return b;
      }
    }
    throw new IllegalArgumentException("Unexpected value '" + value + "'");
  }

  @Override
  public String toString() {
    return String.valueOf(value);
  }
}
