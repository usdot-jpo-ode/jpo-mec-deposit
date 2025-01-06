package us.dot.its.jpo.ode.mec.deposit.etx.models;

import java.io.Serializable;
import java.net.URI;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import us.dot.its.jpo.ode.mec.deposit.etx.models.mqtt.EtxMqttClientInfo;
import us.dot.its.jpo.ode.mec.deposit.etx.models.partner.EtxNetworkType;

/**
 * Configuration data for ETX (Infrastructure Message Processor) client.
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class EtxConfigData implements Serializable {
  /** Path to the configuration file. */
  private String configFilePath;
  /** Path to the CA certificate file. */
  private String caCertPath;
  /** Path to the client certificate file. */
  private String clientCertPath;
  /** Path to the private key file. */
  private String keyFilePath;
  /** ETX vendor identifier. */
  private String impVendor;
  /** Network type for ETX connection. */
  private EtxNetworkType networkType;
  /** URI for MQTT broker connection. */
  private URI etxMqttUri;
  /** Device identifier. */
  private String deviceID;
  /** MQTT client session information. */
  private EtxMqttClientInfo etxSessionID;
}
