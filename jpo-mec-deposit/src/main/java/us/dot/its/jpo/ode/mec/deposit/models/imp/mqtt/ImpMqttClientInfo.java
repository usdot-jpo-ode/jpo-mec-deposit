package us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents information about an MQTT client for IMP (Infrastructure Message Processor).
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class ImpMqttClientInfo {
  /** The session ID of the MQTT client. */
  @JsonProperty("SessionID")
  private String sessionId;
}
