package us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum ImpMqttMessageType {
    BSM("BSM"), PSM("PSM"), RSA("RSA"), TIM("TIM"), MAP("MAP"), SPAT("SPAT");

    private String value;

    ImpMqttMessageType(String value) {
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
    public static ImpMqttMessageType fromValue(String value) {
        for (ImpMqttMessageType b : ImpMqttMessageType.values()) {
            if (b.value.equals(value)) {
                return b;
            }
        }
        throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
}
