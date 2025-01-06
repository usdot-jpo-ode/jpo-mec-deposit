package us.dot.its.jpo.ode.mec.deposit.etx.mqtt;

import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxApi;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxTokenManager;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxUtil;
import us.dot.its.jpo.ode.mec.deposit.etx.models.EtxConfigData;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.mqtt.core.ClientManager;
import org.springframework.integration.mqtt.core.Mqttv3ClientManager;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.outbound.MqttPahoMessageHandler;
import org.springframework.messaging.MessageChannel;
import jakarta.annotation.PostConstruct;

/**
 * Configuration class for setting up MQTT client and message channels for ETX integration.
 */
@Slf4j
@Configuration
@ConditionalOnProperty(value = {"etx.enabled"}, havingValue = "true")
public class EtxMqttConfig {
  private final EtxMqttProperties mqttProperties;
  private final EtxProperties etxProperties;
  private final EtxTokenManager tokenManager;
  private final EtxApi impApi;
  private EtxConfigData impConfig;

  /**
   * Constructs the MQTT configuration with required properties.
   *
   * @param tokenManager The ETX token manager param impApi The ETX API
   */
  public EtxMqttConfig(EtxTokenManager tokenManager, EtxApi impApi,
      EtxMqttProperties mqttProperties, EtxProperties etxProperties) {
    this.tokenManager = tokenManager;
    this.impApi = impApi;
    this.mqttProperties = mqttProperties;
    this.etxProperties = etxProperties;
  }

  @PostConstruct
  public void init() {
    try {
      String token = tokenManager.getValidToken();
      this.impConfig = impApi.registerClientPartner(token);
      if (this.impConfig == null || this.impConfig.getEtxMqttUri() == null) {
        throw new IllegalStateException("Failed to initialize ETX configuration");
      }
      log.info("ETX MQTT configuration initialized successfully");
    } catch (Exception e) {
      log.error("Failed to initialize ETX MQTT configuration", e);
      throw new IllegalStateException("Failed to initialize ETX MQTT configuration", e);
    }
  }

  /**
   * Creates and configures the MQTT client manager with SSL and connection settings.
   *
   * @return Configured MQTT client manager
   */
  @Bean
  public ClientManager<IMqttAsyncClient, MqttConnectOptions> clientManager() {
    EtxConfigData impConfig =
        EtxUtil.readConfigFile(etxProperties.getCertificatePath() + "/config.json");
    String brokerUrl = impConfig.getEtxMqttUri().toString().replace("mqtt://", "ssl://");

    MqttConnectOptions options = new MqttConnectOptions();
    options.setServerURIs(new String[] {brokerUrl});
    options.setCleanSession(true);
    options.setSocketFactory(EtxUtil.createSocketFactory(impConfig.getCaCertPath(),
        impConfig.getClientCertPath(), impConfig.getKeyFilePath()));
    options.setConnectionTimeout(10);
    options.setKeepAliveInterval(30);
    options.setAutomaticReconnect(true);
    options.setMaxInflight(mqttProperties.getMaxInflight());
    options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);

    Mqttv3ClientManager clientManager = new Mqttv3ClientManager(options, impConfig.getDeviceID());
    String tmpDir = etxProperties.getCertificatePath() + "/mqtt-persistence";
    clientManager.setPersistence(new MqttDefaultFilePersistence(tmpDir));
    return clientManager;
  }

  /**
   * Creates a message channel for outbound MQTT messages.
   *
   * @return DirectChannel for MQTT outbound messages
   */
  @Bean
  public MessageChannel mqttOutboundChannel() {
    return new DirectChannel();
  }

  /**
   * Creates and configures the MQTT inbound message flow.
   *
   * @param clientManager The MQTT client manager
   * @param etxProperties ETX configuration properties
   * @return Configured IntegrationFlow for inbound MQTT messages
   */
  @Bean
  public IntegrationFlow mqttInFlow(
      ClientManager<IMqttAsyncClient, MqttConnectOptions> clientManager,
      EtxProperties etxProperties) {

    EtxConfigData impConfig =
        EtxUtil.readConfigFile(etxProperties.getCertificatePath() + "/config.json");

    log.info("Setting up MQTT inbound adapter with deviceID: {}", impConfig.getDeviceID());
    log.info("Subscribing to topics: {}", Arrays.toString(mqttProperties.getSubscriptions()));

    MqttPahoMessageDrivenChannelAdapter messageProducer =
        new MqttPahoMessageDrivenChannelAdapter(clientManager, mqttProperties.getSubscriptions());

    messageProducer.setQos(mqttProperties.getQos());

    return IntegrationFlow.from(messageProducer).channel("mqttInputChannel").get();
  }

  /**
   * Creates and configures the MQTT outbound message handler.
   *
   * @param clientManager The MQTT client manager
   * @return Configured MQTT message handler for outbound messages
   */
  @Bean
  public MqttPahoMessageHandler mqttOutboundMessageHandler(
      ClientManager<IMqttAsyncClient, MqttConnectOptions> clientManager) {
    MqttPahoMessageHandler messageHandler = new MqttPahoMessageHandler(clientManager);
    messageHandler.setAsync(true);
    messageHandler.setDefaultQos(0);
    messageHandler.setDefaultRetained(false);
    return messageHandler;
  }

  /**
   * Creates and configures the MQTT outbound message flow.
   *
   * @param clientManager The MQTT client manager
   * @return Configured IntegrationFlow for outbound messages
   */
  @Bean
  public IntegrationFlow mqttOutFlow(
      ClientManager<IMqttAsyncClient, MqttConnectOptions> clientManager) {
    return f -> f.channel("mqttOutboundChannel").handle(new MqttPahoMessageHandler(clientManager));
  }
}
