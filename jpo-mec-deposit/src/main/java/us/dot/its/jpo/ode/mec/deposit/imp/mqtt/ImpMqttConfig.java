package us.dot.its.jpo.ode.mec.deposit.imp.mqtt;

import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
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
import us.dot.its.jpo.ode.mec.deposit.imp.ImpApi;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpProperties;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpTokenManager;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpUtil;
import us.dot.its.jpo.ode.mec.deposit.models.imp.ImpConfigData;

/**
 * Configuration class for setting up MQTT client and message channels for IMP integration.
 */
@Slf4j
@Configuration
public class ImpMqttConfig {
  private final ImpMqttProperties mqttProperties;
  private final ImpProperties impProperties;
  private final ImpTokenManager tokenManager;
  private final ImpApi impApi;
  private ImpConfigData impConfig;

  /**
   * Constructs the MQTT configuration with required properties.
   *
   * @param tokenManager The IMP token manager
   * @param impApi The IMP API
   */
  public ImpMqttConfig(ImpTokenManager tokenManager, ImpApi impApi, ImpMqttProperties mqttProperties,
      ImpProperties impProperties) {
    this.tokenManager = tokenManager;
    this.impApi = impApi;
    this.mqttProperties = mqttProperties;
    this.impProperties = impProperties;
  }

  @PostConstruct
  public void init() {
    try {
      String token = tokenManager.getValidToken();
      this.impConfig = impApi.registerClientPartner(token);
      if (this.impConfig == null || this.impConfig.getImpMqttUri() == null) {
        throw new IllegalStateException("Failed to initialize IMP configuration");
      }
      log.info("IMP MQTT configuration initialized successfully");
    } catch (Exception e) {
      log.error("Failed to initialize IMP MQTT configuration", e);
      throw new IllegalStateException("Failed to initialize IMP MQTT configuration", e);
    }
  }

  /**
   * Creates and configures the MQTT client manager with SSL and connection settings.
   *
   * @return Configured MQTT client manager
   */
  @Bean
  public ClientManager<IMqttAsyncClient, MqttConnectOptions> clientManager() {
    ImpConfigData impConfig =
        ImpUtil.readConfigFile(impProperties.getCertificatePath() + "/config.json");
    String brokerUrl = impConfig.getImpMqttUri().toString().replace("mqtt://", "ssl://");

    MqttConnectOptions options = new MqttConnectOptions();
    options.setServerURIs(new String[] {brokerUrl});
    options.setCleanSession(true);
    options.setSocketFactory(ImpUtil.createSocketFactory(impConfig.getCaCertPath(),
        impConfig.getClientCertPath(), impConfig.getKeyFilePath()));
    options.setConnectionTimeout(10);
    options.setKeepAliveInterval(30);
    options.setAutomaticReconnect(true);
    options.setMaxInflight(mqttProperties.getMaxInflight());
    options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);

    Mqttv3ClientManager clientManager = new Mqttv3ClientManager(options, impConfig.getDeviceID());
    String tmpDir = impProperties.getCertificatePath() + "/mqtt-persistence";
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
   * @param impProperties IMP configuration properties
   * @return Configured IntegrationFlow for inbound MQTT messages
   */
  @Bean
  public IntegrationFlow mqttInFlow(
      ClientManager<IMqttAsyncClient, MqttConnectOptions> clientManager,
      ImpProperties impProperties) {

    ImpConfigData impConfig =
        ImpUtil.readConfigFile(impProperties.getCertificatePath() + "/config.json");

    log.debug("Setting up MQTT inbound adapter with deviceID: {}", impConfig.getDeviceID());
    log.debug("Subscribing to topics: {}", Arrays.toString(mqttProperties.getSubscriptions()));

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
