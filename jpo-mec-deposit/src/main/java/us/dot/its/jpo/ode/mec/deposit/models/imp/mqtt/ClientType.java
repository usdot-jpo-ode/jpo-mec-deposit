package us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum ClientType {
    // "Vehicle""VulnerableRoadUser""TrafficLightController""InfrastructureSensor""OnboardSensor""Software"
    VEHICLE("Vehicle"), VULNERABLE_ROAD_USER("VulnerableRoadUser"), TRAFFIC_LIGHT_CONTROLLER("TrafficLightController"),
    INFRASTRUCTURE_SENSOR("InfrastructureSensor"), ONBOARD_SENSOR("OnboardSensor"), SOFTWARE("Software");

    private String value;

    ClientType(String value) {
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
    public static ClientType fromValue(String value) {
        for (ClientType b : ClientType.values()) {
            if (b.value.equals(value)) {
                return b;
            }
        }
        throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
}
