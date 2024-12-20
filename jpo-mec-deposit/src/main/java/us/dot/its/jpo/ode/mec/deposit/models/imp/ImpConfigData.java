package us.dot.its.jpo.ode.mec.deposit.models.imp;

import java.io.Serializable;
import java.net.URI;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttClientInfo;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.ImpNetworkType;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class ImpConfigData implements Serializable {
    private String configFilePath;
    private String caCertPath;
    private String clientCertPath;
    private String keyFilePath;
    private String impVendor;
    private ImpNetworkType networkType;
    private URI impMqttUri;
    private String deviceID;
    private ImpMqttClientInfo impSessionID;
}