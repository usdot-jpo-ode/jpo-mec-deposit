package us.dot.its.jpo.ode.mec.deposit.imp;

import org.springframework.integration.mqtt.core.ClientManager;

import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.mqtt.core.Mqttv3ClientManager;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.outbound.MqttPahoMessageHandler;
import org.springframework.messaging.MessageChannel;

import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.ode.mec.deposit.DepositorProperties;
import us.dot.its.jpo.ode.mec.deposit.models.imp.ConfigData;
import us.dot.its.jpo.ode.mec.deposit.utils.CommonUtils;

@Slf4j
@Configuration
public class ImpMqttConfig {

    private final ImpMqttProperties impMqttProperties;

    public ImpMqttConfig(ImpMqttProperties impMqttProperties) {
        this.impMqttProperties = impMqttProperties;
    }

    @Bean
    public ClientManager<IMqttAsyncClient, MqttConnectOptions> clientManager(
            DepositorProperties properties) {
        ConfigData impConfig = CommonUtils
                .readConfigFile(properties.getImpCertPath() + "/config.json");
        String brokerUrl = impConfig.getImpMqttUri().toString().replace("mqtt://", "ssl://");

        MqttConnectOptions options = new MqttConnectOptions();
        options.setServerURIs(new String[] { brokerUrl });
        options.setCleanSession(true);
        options.setSocketFactory(CommonUtils.createSocketFactory(impConfig.getCaCertPath(),
                impConfig.getClientCertPath(), impConfig.getKeyFilePath()));
        options.setConnectionTimeout(10);
        options.setKeepAliveInterval(30);
        options.setAutomaticReconnect(true);
        options.setMaxInflight(1000);

        Mqttv3ClientManager clientManager = new Mqttv3ClientManager(options,
                impConfig.getDeviceID());
        clientManager.setPersistence(new MqttDefaultFilePersistence());
        return clientManager;
    }

    @Bean
    public MessageChannel mqttOutboundChannel() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow mqttInFlow(
            ClientManager<IMqttAsyncClient, MqttConnectOptions> clientManager,
            DepositorProperties properties) {

        ConfigData impConfig = CommonUtils
                .readConfigFile(properties.getImpCertPath() + "/config.json");

        log.debug("Setting up MQTT inbound adapter with deviceID: {}", impConfig.getDeviceID());
        log.debug("Subscribing to topics: {}", impMqttProperties.getSubscriptions());

        MqttPahoMessageDrivenChannelAdapter messageProducer = new MqttPahoMessageDrivenChannelAdapter(
                clientManager, impMqttProperties.getSubscriptions().toArray(new String[0]));

        messageProducer.setQos(impMqttProperties.getQos());

        return IntegrationFlow.from(messageProducer).channel("mqttInputChannel").get();
    }

    @Bean
    public IntegrationFlow mqttOutFlow(
            ClientManager<IMqttAsyncClient, MqttConnectOptions> clientManager) {
        return f -> f.channel("mqttOutboundChannel")
                .handle(new MqttPahoMessageHandler(clientManager));
    }
}