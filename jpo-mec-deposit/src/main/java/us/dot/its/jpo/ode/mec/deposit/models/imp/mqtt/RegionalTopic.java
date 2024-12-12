package us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@AllArgsConstructor
@Data
@Builder
public class RegionalTopic {
    private String mqttGeohash;
    private String vendorId;
    private MessageFormat messageFormat;
    private MessageType messageType;
    private ClientType clientType;
    private ClientSubType clientSubType;
}