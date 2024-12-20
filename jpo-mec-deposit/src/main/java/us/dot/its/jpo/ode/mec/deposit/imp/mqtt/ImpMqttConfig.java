package us.dot.its.jpo.ode.mec.deposit.imp.mqtt;

import org.springframework.integration.mqtt.core.ClientManager;

import java.util.Arrays;

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
import us.dot.its.jpo.ode.mec.deposit.imp.ImpProperties;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpUtil;
import us.dot.its.jpo.ode.mec.deposit.models.imp.ImpConfigData;

@Slf4j
@Configuration
public class ImpMqttConfig {

    private final ImpMqttProperties mqttProperties;
    private final ImpProperties impProperties;

    public ImpMqttConfig(ImpMqttProperties mqttProperties, ImpProperties impProperties) {
        this.mqttProperties = mqttProperties;
        this.impProperties = impProperties;
    }

    @Bean
    public ClientManager<IMqttAsyncClient, MqttConnectOptions> clientManager() {
        ImpConfigData impConfig = ImpUtil
                .readConfigFile(impProperties.getCertificatePath() + "/config.json");
        String brokerUrl = impConfig.getImpMqttUri().toString().replace("mqtt://", "ssl://");

        MqttConnectOptions options = new MqttConnectOptions();
        options.setServerURIs(new String[] { brokerUrl });
        options.setCleanSession(true);
        options.setSocketFactory(ImpUtil.createSocketFactory(impConfig.getCaCertPath(),
                impConfig.getClientCertPath(), impConfig.getKeyFilePath()));
        options.setConnectionTimeout(10);
        options.setKeepAliveInterval(30);
        options.setAutomaticReconnect(true);
        options.setMaxInflight(mqttProperties.getMaxInflight());
        options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);

        Mqttv3ClientManager clientManager = new Mqttv3ClientManager(options,
                impConfig.getDeviceID());
        String tmpDir = impProperties.getCertificatePath() + "/mqtt-persistence";
        clientManager.setPersistence(new MqttDefaultFilePersistence(tmpDir));
        return clientManager;
    }

    @Bean
    public MessageChannel mqttOutboundChannel() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow mqttInFlow(
            ClientManager<IMqttAsyncClient, MqttConnectOptions> clientManager,
            ImpProperties impProperties) {

        ImpConfigData impConfig = ImpUtil
                .readConfigFile(impProperties.getCertificatePath() + "/config.json");

        log.debug("Setting up MQTT inbound adapter with deviceID: {}", impConfig.getDeviceID());
        log.debug("Subscribing to topics: {}", Arrays.toString(mqttProperties.getSubscriptions()));

        MqttPahoMessageDrivenChannelAdapter messageProducer = new MqttPahoMessageDrivenChannelAdapter(
                clientManager, mqttProperties.getSubscriptions());

        messageProducer.setQos(mqttProperties.getQos());

        return IntegrationFlow.from(messageProducer).channel("mqttInputChannel").get();
    }

    @Bean
    public IntegrationFlow mqttOutFlow(
            ClientManager<IMqttAsyncClient, MqttConnectOptions> clientManager) {
        return f -> f.channel("mqttOutboundChannel")
                .handle(new MqttPahoMessageHandler(clientManager));
    }
}