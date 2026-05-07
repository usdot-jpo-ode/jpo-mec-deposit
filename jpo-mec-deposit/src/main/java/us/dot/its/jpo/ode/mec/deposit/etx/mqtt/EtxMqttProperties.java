package us.dot.its.jpo.ode.mec.deposit.etx.mqtt;

import java.util.List;
import lombok.Data;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttBrokerType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttBrokerTarget;

/**
 * ETX MQTT connection and messaging settings (bound under
 * {@code mec-deposit.etx.mqtt-brokers.etx.mqtt}).
 */
@Data
public class EtxMqttProperties {
  private int qos;
  private int maxInflight;
  private int connectionTimeout;
  private int keepAliveInterval;
  private int completionTimeout;
  private int maxMessagesPerSecond;
  private String vendor;
  private int precision;
  private EtxMqttMessageFormat messageFormat;
  private String[] subscriptions;
  private int registrationRefreshFailureThreshold;
  private boolean useTls = true;
  private boolean useRegistration = true;
  private boolean requireSessionId = true;
  private String brokerUri;
  private String clientId;
  private EtxMqttBrokerType brokerType = EtxMqttBrokerType.ETX;
  private boolean dualPublishEnabled = false;
  private List<MqttBrokerTarget> dualPublishTargets =
      List.of(MqttBrokerTarget.ETX, MqttBrokerTarget.NMI, MqttBrokerTarget.AV);
}
