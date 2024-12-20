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
import us.dot.its.jpo.ode.mec.deposit.DepositorProperties;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpUtil;
import us.dot.its.jpo.ode.mec.deposit.models.imp.ImpConfigData;

@Slf4j
@Configuration
public class ImpMqttConfig {

        private final MqttProperties mqttProperties;

        public ImpMqttConfig(MqttProperties mqttProperties) {
                this.mqttProperties = mqttProperties;
        }

        @Bean
        public ClientManager<IMqttAsyncClient, MqttConnectOptions> clientManager(
                        DepositorProperties properties) {
                ImpConfigData impConfig = ImpUtil
                                .readConfigFile(properties.getImpCertPath() + "/config.json");
                String brokerUrl = impConfig.getImpMqttUri().toString().replace("mqtt://",
                                "ssl://");

                MqttConnectOptions options = new MqttConnectOptions();
                options.setServerURIs(new String[] { brokerUrl });
                options.setCleanSession(true);
                options.setSocketFactory(ImpUtil.createSocketFactory(impConfig.getCaCertPath(),
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

                ImpConfigData impConfig = ImpUtil
                                .readConfigFile(properties.getImpCertPath() + "/config.json");

                log.debug("Setting up MQTT inbound adapter with deviceID: {}",
                                impConfig.getDeviceID());
                log.debug("Subscribing to topics: {}",
                                Arrays.toString(mqttProperties.getSubscriptions()));

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