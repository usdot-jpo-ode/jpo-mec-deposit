package us.dot.its.jpo.ode.mec.deposit.models.imp;

import java.io.Serializable;
import java.net.URI;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttClientInfo;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.ImpNetworkType;

/**
 * Configuration data for IMP (Infrastructure Message Processor) client.
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class ImpConfigData implements Serializable {
  /** Path to the configuration file. */
  private String configFilePath;
  /** Path to the CA certificate file. */
  private String caCertPath;
  /** Path to the client certificate file. */
  private String clientCertPath;
  /** Path to the private key file. */
  private String keyFilePath;
  /** IMP vendor identifier. */
  private String impVendor;
  /** Network type for IMP connection. */
  private ImpNetworkType networkType;
  /** URI for MQTT broker connection. */
  private URI impMqttUri;
  /** Device identifier. */
  private String deviceID;
  /** MQTT client session information. */
  private ImpMqttClientInfo impSessionID;
}
