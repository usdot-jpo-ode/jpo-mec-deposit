package us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum ImpMqttMessageFormat {
    J2735("j2735"), J2735_GR("j2735_gr"), AVRO("avro"), JSON("json");

    private String value;

    ImpMqttMessageFormat(String value) {
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
    public static ImpMqttMessageFormat fromValue(String value) {
        for (ImpMqttMessageFormat b : ImpMqttMessageFormat.values()) {
            if (b.value.equals(value)) {
                return b;
            }
        }
        throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
}
