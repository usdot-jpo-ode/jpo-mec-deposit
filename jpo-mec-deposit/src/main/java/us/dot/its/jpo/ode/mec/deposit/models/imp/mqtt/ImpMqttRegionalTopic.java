package us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import us.dot.its.jpo.ode.mec.deposit.models.imp.ImpClientSubType;
import us.dot.its.jpo.ode.mec.deposit.models.imp.ImpClientType;

@AllArgsConstructor
@Data
@Builder
public class ImpMqttRegionalTopic {
    private String mqttGeohash;
    private String vendorId;
    private ImpMqttMessageFormat messageFormat;
    private ImpMqttMessageType messageType;
    private ImpClientType clientType;
    private ImpClientSubType clientSubType;
    private final ImpMqttNamespace namespace = ImpMqttNamespace.REGIONAL;
}