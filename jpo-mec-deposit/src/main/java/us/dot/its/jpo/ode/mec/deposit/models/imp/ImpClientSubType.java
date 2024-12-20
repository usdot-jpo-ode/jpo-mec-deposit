package us.dot.its.jpo.ode.mec.deposit.models.imp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum ImpClientSubType {
    PASSENGER_CAR("PassengerCar"), TRUCK("Truck"), BUS("Bus"),
    EMERGENCY_VEHICLE("EmergencyVehicle"), SCHOOL_BUS("SchoolBus"),
    MAINTENANCE_VEHICLE("MaintenanceVehicle"), PEDESTRIAN("Pedestrian"), BICYCLE("Bicycle"),
    SCOOTER("Scooter"), MOTORCYCLE("Motorcycle"), ROAD_SIDE_UNIT("RoadSideUnit"), CAMERA("Camera"),
    LIDAR("Lidar"), RADAR("Radar"), INDUCTIVE_LOOP("InductiveLoop"),
    MAGNETIC_SENSOR("MagneticSensor"), PLATFORM("Platform"), APPLICATION("Application"), NA("NA");

    private String value;

    ImpClientSubType(String value) {
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
    public static ImpClientSubType fromValue(String value) {
        for (ImpClientSubType b : ImpClientSubType.values()) {
            if (b.value.equals(value)) {
                return b;
            }
        }
        throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
}
