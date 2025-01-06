package us.dot.its.jpo.ode.mec.deposit.etx.mqtt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxUtil;
import us.dot.its.jpo.ode.mec.deposit.etx.models.EtxConfigData;
import us.dot.its.jpo.ode.mec.deposit.etx.models.mqtt.EtxMqttClientInfo;
import us.dot.its.jpo.ode.mec.deposit.utils.DateJsonMapper;

/**
 * Service for handling MQTT subscription messages and client information updates.
 */
@Slf4j
@Service
public class EtxMqttSubscriptionService {
  private final ObjectMapper mapper = DateJsonMapper.getInstance();
  private String configPath;

  /**
   * Constructs the MQTT subscription service.
   *
   * @param etxProperties Properties containing configuration paths
   */
  public EtxMqttSubscriptionService(EtxProperties etxProperties) {
    this.configPath = etxProperties.getCertificatePath() + "/config.json";
  }

  @PostConstruct
  public void init() {
    log.info("EtxMqttSubscriptionService initialized");
  }

  /**
   * Handles incoming MQTT messages from subscribed topics.
   *
   * @param message The received MQTT message
   */
  @ServiceActivator(inputChannel = "mqttInputChannel")
  public void handleMessage(Message<?> message) {
    String topic = (String) message.getHeaders().get("mqtt_receivedTopic");
    String payload = message.getPayload().toString();

    log.info("Received message from topic {}", topic);
    log.debug("Message payload: {}", payload);
    log.debug("Message headers: {}", message.getHeaders());

    switch (topic) {
      case "vzimp/1/ClientInfo":
        try {
          EtxMqttClientInfo clientInfo = mapper.readValue(payload, EtxMqttClientInfo.class);
          handleClientInfo(clientInfo);
        } catch (JsonProcessingException e) {
          log.error("Error parsing ClientInfo message: {}", e.getMessage());
        }
        break;
      default:
        log.info("Unhandled topic: {}", topic);
    }
  }

  private void handleClientInfo(EtxMqttClientInfo payload) {
    try {
      log.info("Processing client info: {}", payload);
      EtxConfigData configData = EtxUtil.readConfigFile(configPath);
      configData.setEtxSessionID(payload);
      EtxUtil.writeToFile(configData.getConfigFilePath(), mapper.writeValueAsString(configData));
    } catch (Exception e) {
      log.error("Error handling client info message", e);
    }
  }
}
