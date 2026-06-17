package us.dot.its.jpo.ode.mec.deposit.config.mqtt;

import jakarta.annotation.PostConstruct;
import java.util.Arrays;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.concurrent.Executors;
import org.springframework.integration.channel.ExecutorChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.mqtt.core.ClientManager;
import org.springframework.integration.mqtt.core.Mqttv3ClientManager;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.outbound.MqttPahoMessageHandler;
import org.springframework.messaging.MessageChannel;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxUtil;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxPartnerApiProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxPartnerClient;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxTokenManager;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.RegistrationConfiguration;

/**
 * Configuration class for setting up MQTT client and message channels for ETX integration.
 */
@Slf4j
@Configuration
@ConditionalOnProperty(value = {"mec-deposit.etx.enabled"}, havingValue = "true")
public class EtxMqttBrokerConfig {
  private final EtxMqttProperties mqttProperties;
  private final EtxPartnerApiProperties partnerApiProperties;
  private final EtxTokenManager tokenManager;
  private final EtxPartnerClient etxApi;
  private RegistrationConfiguration etxConfig;

  /**
   * Constructs the MQTT configuration with required properties.
   *
   * @param tokenManager The ETX token manager param etxApi The ETX API
   */
  public EtxMqttBrokerConfig(EtxTokenManager tokenManager, EtxPartnerClient etxApi,
      EtxMqttProperties mqttProperties, EtxPartnerApiProperties partnerApi) {
    this.tokenManager = tokenManager;
    this.etxApi = etxApi;
    this.mqttProperties = mqttProperties;
    this.partnerApiProperties = partnerApi;
  }

  /**
   * Initializes the MQTT configuration.
   */
  @PostConstruct
  public void init() {
    if (!mqttProperties.isUseRegistration()) {
      log.info("ETX MQTT registration disabled; using configured broker URI: {}",
          mqttProperties.getBrokerUri());
      return;
    }
    try {
      String token = tokenManager.getValidToken();
      this.etxConfig = etxApi.registerClientPartner(token);
      if (this.etxConfig == null || this.etxConfig.getEtxMqttUri() == null) {
        throw new IllegalStateException("Failed to initialize ETX configuration");
      }
      log.info("ETX MQTT configuration initialized successfully");
    } catch (Exception e) {
      log.error("Failed to initialize ETX MQTT configuration", e);
      throw new IllegalStateException("Failed to initialize ETX MQTT configuration", e);
    }
  }

  /**
   * Refreshes the ETX registration by re-registering the client and updating the configuration.
   * This method can be called when MQTT publishes start failing to recover from invalid
   * registrations or connection issues.
   *
   * @return true if refresh was successful, false otherwise
   */
  public synchronized boolean refreshRegistration() {
    try {
      log.info("Refreshing ETX registration...");
      String token = tokenManager.getValidToken();
      RegistrationConfiguration newConfig = etxApi.registerClientPartner(token);
      if (newConfig == null || newConfig.getEtxMqttUri() == null) {
        log.error("Failed to refresh ETX registration - invalid configuration returned");
        return false;
      }
      this.etxConfig = newConfig;
      log.info("ETX registration refreshed successfully. New MQTT URI: {}",
          newConfig.getEtxMqttUri());
      // Note: The MQTT client manager will automatically reconnect with the new configuration
      // since it reads from the config file and has automaticReconnect enabled
      return true;
    } catch (Exception e) {
      log.error("Failed to refresh ETX registration", e);
      return false;
    }
  }

  /**
   * Creates and configures the MQTT client manager with SSL and connection settings.
   *
   * @return Configured MQTT client manager
   */
  @Bean
  public ClientManager<IMqttAsyncClient, MqttConnectOptions> clientManager() {
    RegistrationConfiguration etxConfig = null;
    if (mqttProperties.isUseRegistration()) {
      etxConfig = EtxUtil.readConfigFile(partnerApiProperties.getCertificatePath() + "/config.json");
    }

    String configuredBrokerUri = mqttProperties.getBrokerUri();
    String brokerUrl = configuredBrokerUri;
    if (brokerUrl == null || brokerUrl.isBlank()) {
      if (etxConfig == null || etxConfig.getEtxMqttUri() == null) {
        throw new IllegalStateException("No MQTT broker URI configured");
      }
      brokerUrl = etxConfig.getEtxMqttUri().toString();
    }

    if (mqttProperties.isUseTls() && brokerUrl.startsWith("mqtt://")) {
      brokerUrl = brokerUrl.replace("mqtt://", "ssl://");
    }

    MqttConnectOptions options = new MqttConnectOptions();
    options.setServerURIs(new String[] {brokerUrl});
    options.setCleanSession(true);
    if (mqttProperties.isUseTls()) {
      if (etxConfig == null) {
        throw new IllegalStateException("TLS MQTT requires registration certificate configuration");
      }
      options.setSocketFactory(EtxUtil.createSocketFactory(etxConfig.getCaCertPath(),
          etxConfig.getClientCertPath(), etxConfig.getKeyFilePath()));
    }
    options.setConnectionTimeout(10);
    options.setKeepAliveInterval(30);
    options.setAutomaticReconnect(true);
    options.setMaxInflight(mqttProperties.getMaxInflight());
    options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);

    String mqttClientId = mqttProperties.getClientId();
    if (mqttClientId == null || mqttClientId.isBlank()) {
      mqttClientId = etxConfig != null && etxConfig.getDeviceID() != null && !etxConfig.getDeviceID()
          .isBlank() ? etxConfig.getDeviceID() : "jpo-mec-deposit-" + UUID.randomUUID();
    }

    Mqttv3ClientManager clientManager = new Mqttv3ClientManager(options, mqttClientId);
    String tmpDir = partnerApiProperties.getCertificatePath() + "/mqtt-persistence";
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
    return new ExecutorChannel(Executors.newCachedThreadPool());
  }

  /**
   * Creates and configures the MQTT inbound message flow.
   *
   * @param clientManager The MQTT client manager
   * @return Configured IntegrationFlow for inbound MQTT messages
   */
  @Bean
  public IntegrationFlow mqttInFlow(
      ClientManager<IMqttAsyncClient, MqttConnectOptions> clientManager,
      EtxPartnerApiProperties partnerApi) {
    String configuredClientId = mqttProperties.getClientId();
    String logClientId = configuredClientId;
    if ((logClientId == null || logClientId.isBlank()) && mqttProperties.isUseRegistration()) {
      RegistrationConfiguration etxConfig =
          EtxUtil.readConfigFile(partnerApi.getCertificatePath() + "/config.json");
      logClientId = etxConfig != null ? etxConfig.getDeviceID() : null;
    }
    log.info("Setting up MQTT inbound adapter with clientID: {}", logClientId);
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
    messageHandler.setAsyncEvents(true);
    messageHandler.setDefaultQos(0);
    messageHandler.setDefaultRetained(false);
    return messageHandler;
  }

  /**
   * Creates and configures the MQTT outbound message flow.
   *
   * <p>The {@code mqttOutboundChannel} bean is injected by reference (not by string name) so the
   * DSL uses the pre-existing {@link ExecutorChannel} instead of creating a new
   * {@code DirectChannel} that would override it and make all publishes synchronous on the calling
   * thread.
   *
   * @param mqttOutboundChannel The executor-backed outbound channel
   * @param mqttOutboundMessageHandler The async MQTT outbound message handler
   * @return Configured IntegrationFlow for outbound messages
   */
  @Bean
  public IntegrationFlow mqttOutFlow(MessageChannel mqttOutboundChannel,
      MqttPahoMessageHandler mqttOutboundMessageHandler) {
    return IntegrationFlow.from(mqttOutboundChannel)
        .handle(mqttOutboundMessageHandler)
        .get();
  }
}
