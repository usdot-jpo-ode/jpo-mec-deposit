package us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@AllArgsConstructor
@Data
@Builder
public class ImpMqttRegionalTopic {
    private String mqttGeohash;
    private String vendorId;
    private ImpMqttMessageFormat messageFormat;
    private ImpMqttMessageType messageType;
    private ImpMqttClientType clientType;
    private ImpMqttClientSubType clientSubType;
}