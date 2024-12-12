package us.dot.its.jpo.ode.mec.deposit.imp;

import java.io.File;
import java.nio.file.Paths;

import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;
import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.pem.util.PemUtils;
import us.dot.its.jpo.ode.mec.deposit.GeoRoutedMsg;
import us.dot.its.jpo.ode.mec.deposit.DepositorProperties;
import us.dot.its.jpo.ode.mec.deposit.models.imp.ConfigData;
import us.dot.its.jpo.ode.mec.deposit.utils.CommonUtils;

@Slf4j
public class ImpMqttService {
    private MqttClient client;
    private final String brokerUri;
    private final String clientId;
    private final MqttConnectOptions options;
    private final MqttDefaultFilePersistence persistence;

    public ImpMqttService(DepositorProperties properties) {
        waitForFiles(properties.getImpCertPath());
        ConfigData impConfig = CommonUtils.readConfigFile(properties.getImpCertPath() + "/config.json");
        this.brokerUri = impConfig.getImpMqttUri().toString().replace("mqtt://", "ssl://");
        this.clientId = impConfig.getDeviceID();
        String persistenceDir = properties.getImpCertPath(); // Add a method to get the persistence directory from
                                                             // properties
        this.persistence = new MqttDefaultFilePersistence(persistenceDir);

        SSLSocketFactory socketFactory = createSocketFactory(impConfig.getCaCertPath(), impConfig.getClientCertPath(),
                impConfig.getKeyFilePath());

        this.options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setSocketFactory(socketFactory);
        options.setConnectionTimeout(30);
        options.setKeepAliveInterval(60);
        options.setAutomaticReconnect(true);
        options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);

        try {
            connect();
        } catch (MqttException e) {
            log.error("Error while connecting to broker. Reason Code: {}, Message: {}", e.getReasonCode(),
                    e.getMessage(), e);
        }
    }

    private void connect() throws MqttException {
        client = new MqttClient(brokerUri, clientId, persistence);
        client.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                log.error("Connection lost", cause);
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                // Handle incoming messages
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                // Handle delivery completion
            }

        });

        client.connect(options);
    }

    private void waitForFiles(String filePath) {
        try {
            Thread.sleep(5000);
            File configFile = new File(filePath + "/config.json");
            File caCertFile = new File(filePath + "/imp-ca.pem");
            File clientCertFile = new File(filePath + "/imp-cert.pem");
            File keyFile = new File(filePath + "/imp-key.pem");
            while (!configFile.exists() || !caCertFile.exists() || !clientCertFile.exists() || !keyFile.exists()) {
                Thread.sleep(1000); // Wait for 1 second before checking again
            }
            log.info("All files found");
        } catch (InterruptedException e) {
            log.error("Error while waiting for config file", e);
        }
    }

    public static SSLSocketFactory createSocketFactory(String caCertPath, String clientCertPath,
            String privateKeyPath) {
        // Convert to absolute paths
        String absoluteCaCertPath = Paths.get(caCertPath).toAbsolutePath().toString();
        String absoluteClientCertPath = Paths.get(clientCertPath).toAbsolutePath().toString();
        String absolutePrivateKeyPath = Paths.get(privateKeyPath).toAbsolutePath().toString();

        log.info("CA Cert Path: {}", absoluteCaCertPath);
        log.info("Client Cert Path: {}", absoluteClientCertPath);
        log.info("Private Key Path: {}", absolutePrivateKeyPath);

        // Check if files exist
        if (!new File(absoluteCaCertPath).exists()) {
            throw new IllegalArgumentException("CA Certificate file not found at path: " + absoluteCaCertPath);
        }
        if (!new File(absoluteClientCertPath).exists()) {
            throw new IllegalArgumentException("Client Certificate file not found at path: " + absoluteClientCertPath);
        }
        if (!new File(absolutePrivateKeyPath).exists()) {
            throw new IllegalArgumentException("Private Key file not found at path: " + absolutePrivateKeyPath);
        }

        X509ExtendedKeyManager keyManager = PemUtils.loadIdentityMaterial(Paths.get(absoluteClientCertPath),
                Paths.get(absolutePrivateKeyPath));
        X509ExtendedTrustManager trustManager = PemUtils.loadTrustMaterial(Paths.get(absoluteCaCertPath));

        var sslFactory = SSLFactory.builder().withIdentityMaterial(keyManager).withTrustMaterial(trustManager).build();

        var sslSocketFactory = sslFactory.getSslSocketFactory();
        return sslSocketFactory;
    }

    public void publishAsn1Bytes(String topic, byte[] asn1Bytes, boolean retain) throws MqttException {
        try {
            log.debug("Publishing message to topic: {}", topic);
            client.publish(topic, asn1Bytes, 0, retain);
        } catch (MqttException e) {
            log.error("Error while publishing message. Reason Code: {}, Message: {}", e.getReasonCode(), e.getMessage(),
                    e);
            throw e;
        }
    }
}