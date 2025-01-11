package us.dot.its.jpo.ode.mec.deposit.models.etx.partner;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Gets or Sets NetworkType
 */

public enum EtxNetworkType {

  NON_VZ("non-VZ"),

  VZ("VZ");

  private String value;

  EtxNetworkType(String value) {
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
  public static EtxNetworkType fromValue(String value) {
    for (EtxNetworkType b : EtxNetworkType.values()) {
      if (b.value.equals(value)) {
        return b;
      }
    }
    throw new IllegalArgumentException("Unexpected value '" + value + "'");
  }
}
