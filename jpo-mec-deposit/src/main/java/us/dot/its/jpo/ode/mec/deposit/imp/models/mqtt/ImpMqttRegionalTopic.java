package us.dot.its.jpo.ode.mec.deposit.imp.models.mqtt;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import us.dot.its.jpo.ode.mec.deposit.imp.models.ImpClientSubType;
import us.dot.its.jpo.ode.mec.deposit.imp.models.ImpClientType;

/**
 * Represents a regional MQTT topic for IMP messaging.
 */
@AllArgsConstructor
@Data
@Builder
public class ImpMqttRegionalTopic {
  /** Geohash for the regional topic. */
  private String mqttGeohash;
  /** Vendor identifier. */
  private String vendorId;
  /** Format of the MQTT message. */
  private ImpMqttMessageFormat messageFormat;
  /** Type of the MQTT message. */
  private ImpMqttMessageType messageType;
  /** Type of the IMP client. */
  private ImpClientType clientType;
  /** Subtype of the IMP client. */
  private ImpClientSubType clientSubType;
  /** Namespace for the MQTT topic, always REGIONAL. */
  private final ImpMqttNamespace namespace = ImpMqttNamespace.REGIONAL;
}
