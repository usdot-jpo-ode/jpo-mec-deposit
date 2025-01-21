package us.dot.its.jpo.ode.mec.deposit.models.etx.partner;

import java.io.Serializable;
import java.math.BigDecimal;
import java.net.URI;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientSubType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttClientInfo;

/**
 * Registration configuration for ETX (Edge Traffic Exchange) client. This class is used to cache
 * the registration configuration for the ETX client.
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class RegistrationConfiguration implements Serializable {
  private String configFilePath;
  private String caCertPath;
  private String clientCertPath;
  private String keyFilePath;
  private String etxVendor;
  private EtxClientType clientType;
  private EtxClientSubType clientSubType;
  private NetworkType networkType;
  private URI etxMqttUri;
  private String deviceID;
  private EtxMqttClientInfo etxSessionID;
  private BigDecimal mecLatitude;
  private BigDecimal mecLongitude;

}
