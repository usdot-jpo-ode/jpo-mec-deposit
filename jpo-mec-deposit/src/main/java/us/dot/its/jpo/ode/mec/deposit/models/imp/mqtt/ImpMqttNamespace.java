package us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum ImpMqttNamespace {
    GEO_RELEVANCE("GeoRelevance"), REGIONAL("Regional"), PRIVATE("Private");

    private final String value;

    ImpMqttNamespace(String value) {
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
    public static ImpMqttNamespace fromValue(String value) {
        for (ImpMqttNamespace b : ImpMqttNamespace.values()) {
            if (b.value.equals(value)) {
                return b;
            }
        }
        throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }

}
