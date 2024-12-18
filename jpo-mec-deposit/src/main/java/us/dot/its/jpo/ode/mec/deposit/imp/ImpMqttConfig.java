package us.dot.its.jpo.ode.mec.deposit.imp;

import javax.net.ssl.SSLSocketFactory;

import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.outbound.MqttPahoMessageHandler;
import org.springframework.messaging.MessageChannel;
import org.springframework.retry.annotation.EnableRetry;

import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.ode.mec.deposit.DepositorProperties;
import us.dot.its.jpo.ode.mec.deposit.models.imp.ConfigData;
import us.dot.its.jpo.ode.mec.deposit.utils.CommonUtils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
@Configuration
public class ImpMqttConfig {

    @Bean
    public MqttPahoClientFactory mqttClientFactory(DepositorProperties properties) {
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();

        ConfigData impConfig = CommonUtils.readConfigFile(properties.getImpCertPath() + "/config.json");
        String brokerUrl = impConfig.getImpMqttUri().toString().replace("mqtt://", "ssl://");

        MqttConnectOptions options = new MqttConnectOptions();
        options.setServerURIs(new String[] { brokerUrl });
        options.setCleanSession(true);
        options.setSocketFactory(CommonUtils.createSocketFactory(impConfig.getCaCertPath(),
                impConfig.getClientCertPath(), impConfig.getKeyFilePath()));
        options.setConnectionTimeout(10);
        options.setKeepAliveInterval(30);
        options.setAutomaticReconnect(true);
        options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
        options.setMaxInflight(1000);

        factory.setConnectionOptions(options);
        return factory;
    }

    @Bean
    public MessageChannel mqttOutboundChannel() {
        return new DirectChannel();
    }

    @Bean
    @ServiceActivator(inputChannel = "mqttOutboundChannel")
    public MqttPahoMessageHandler mqttOutbound(MqttPahoClientFactory mqttClientFactory,
            DepositorProperties properties) {
        ConfigData impConfig = CommonUtils.readConfigFile(properties.getImpCertPath() + "/config.json");
        MqttPahoMessageHandler messageHandler = new MqttPahoMessageHandler(impConfig.getDeviceID(), mqttClientFactory);
        messageHandler.setAsync(true);
        messageHandler.setDefaultQos(0);
        messageHandler.setCompletionTimeout(5000);
        return messageHandler;
    }
}