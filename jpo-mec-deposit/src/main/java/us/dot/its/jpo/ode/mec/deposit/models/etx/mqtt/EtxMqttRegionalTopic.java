package us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientSubType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientType;

/**
 * Represents a regional MQTT topic for ETX messaging.
 */
@AllArgsConstructor
@Data
@Builder
public class EtxMqttRegionalTopic {
  /** Geohash for the regional topic. */
  private String mqttGeohash;
  /** Vendor identifier. */
  private String vendorId;
  /** Format of the MQTT message. */
  private EtxMqttMessageFormat messageFormat;
  /** Type of the MQTT message. */
  private EtxMqttMessageType messageType;
  /** Type of the ETX client. */
  private EtxClientType clientType;
  /** Subtype of the ETX client. */
  private EtxClientSubType clientSubType;
  /** Namespace for the MQTT topic, always REGIONAL. */
  private final EtxMqttNamespace namespace = EtxMqttNamespace.REGIONAL;
}
