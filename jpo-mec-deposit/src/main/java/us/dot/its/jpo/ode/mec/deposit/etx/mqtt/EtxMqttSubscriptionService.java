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
  protected final ObjectMapper mapper;
  private final String configPath;

  /**
   * Constructor for EtxMqttSubscriptionService.
   *
   * @param etxProperties Properties containing configuration paths
   * @param mapper The ObjectMapper instance
   */
  public EtxMqttSubscriptionService(EtxProperties etxProperties) {
    this.configPath = etxProperties.getCertificatePath() + "/config.json";
    this.mapper = DateJsonMapper.getInstance();
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
    try {
      String topic = (String) message.getHeaders().get("mqtt_receivedTopic");
      String payload = message.getPayload().toString();

      log.info("Received message from topic {}", topic);
      log.debug("Message payload: {}", payload);
      log.debug("Message headers: {}", message.getHeaders());

      if ("vzimp/1/ClientInfo".equals(topic)) {
        try {
          EtxMqttClientInfo clientInfo = mapper.readValue(payload, EtxMqttClientInfo.class);
          handleClientInfo(clientInfo);
        } catch (JsonProcessingException e) {
          log.error("Failed to parse client info payload: {}", payload, e);
        }
      } else {
        log.warn("Unhandled topic: {}", topic);
      }
    } catch (Exception e) {
      log.error("Error processing message: {}", message, e);
    }
  }

  private void handleClientInfo(EtxMqttClientInfo payload) {
    try {
      log.info("Processing client info: {}", payload);
      EtxConfigData configData = EtxUtil.readConfigFile(configPath);
      configData.setEtxSessionID(payload);
      EtxUtil.writeToFile(configData.getConfigFilePath(), mapper.writeValueAsString(configData));
    } catch (Exception e) {
      log.error("Failed to handle client info: {}", payload, e);
    }
  }
}
