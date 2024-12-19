package us.dot.its.jpo.ode.mec.deposit.models.imp;

import java.io.Serializable;
import java.net.URI;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ClientInfo;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.NetworkType;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class ConfigData implements Serializable {
    private String configFilePath;
    private String caCertPath;
    private String clientCertPath;
    private String keyFilePath;
    private String impVendor;
    private NetworkType networkType;
    private URI impMqttUri;
    private String deviceID;
    private ClientInfo impSessionID;
}