package us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonProperty;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class ImpMqttClientInfo {
    @JsonProperty("SessionID")
    private String sessionId;
}
