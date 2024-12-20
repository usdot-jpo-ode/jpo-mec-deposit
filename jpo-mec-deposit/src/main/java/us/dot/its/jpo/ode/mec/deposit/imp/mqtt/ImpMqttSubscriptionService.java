package us.dot.its.jpo.ode.mec.deposit.imp.mqtt;

import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpProperties;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpUtil;
import us.dot.its.jpo.ode.mec.deposit.models.imp.ImpConfigData;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttClientInfo;
import us.dot.its.jpo.ode.mec.deposit.utils.DateJsonMapper;

@Slf4j
@Service
public class ImpMqttSubscriptionService {
    private final ObjectMapper mapper = DateJsonMapper.getInstance();
    private String configPath;

    public ImpMqttSubscriptionService(ImpProperties impProperties) {
        this.configPath = impProperties.getCertificatePath() + "/config.json";
    }

    @PostConstruct
    public void init() {
        log.info("ImpMqttSubscriptionService initialized");
    }

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
                ImpMqttClientInfo clientInfo = mapper.readValue(payload, ImpMqttClientInfo.class);
                handleClientInfo(clientInfo);
            } catch (JsonProcessingException e) {
                log.error("Error parsing ClientInfo message: {}", e.getMessage());
            }
            break;
        default:
            log.info("Unhandled topic: {}", topic);
        }
    }

    private void handleClientInfo(ImpMqttClientInfo payload) {
        try {
            log.info("Processing client info: {}", payload);
            ImpConfigData configData = ImpUtil.readConfigFile(configPath);
            configData.setImpSessionID(payload);
            ImpUtil.writeToFile(configData.getConfigFilePath(),
                    mapper.writeValueAsString(configData));
        } catch (Exception e) {
            log.error("Error handling client info message", e);
        }
    }
}