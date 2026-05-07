package us.dot.its.jpo.ode.mec.deposit.services.nmi.mqtt;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.stereotype.Service;
import us.dot.its.jpo.ode.mec.deposit.models.nmi.mqtt.NmiMqttProperties;

/**
 * Dedicated NMI MQTT publish service.
 */
@Slf4j
@Service
public class NmiMqttPublishService {
  private final NmiMqttProperties properties;
  private final AtomicInteger availableTokens;
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private final Counter circuitBreakerSkippedCounter;
  private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
  private volatile boolean circuitOpen = false;
  private MqttClient mqttClient;

  /**
   * Creates NMI MQTT publish service.
   */
  public NmiMqttPublishService(NmiMqttProperties properties, MeterRegistry registry) {
    this.properties = properties;
    this.availableTokens = new AtomicInteger(Math.max(properties.getMaxMessagesPerSecond(), 1));
    this.circuitBreakerSkippedCounter = Counter.builder("mec-deposit.nmi.mqtt.circuit.skipped")
        .description("Messages skipped while NMI circuit breaker is open").register(registry);
    scheduler.scheduleAtFixedRate(() -> availableTokens.set(Math.max(properties.getMaxMessagesPerSecond(), 1)),
        1, 1, TimeUnit.SECONDS);
  }

  /**
   * Publishes binary ASN.1 payload to NMI broker.
   */
  public synchronized void publishAsn1Bytes(String topic, byte[] payload, boolean retain) {
    if (!properties.isEnabled()) {
      return;
    }
    if (circuitOpen) {
      circuitBreakerSkippedCounter.increment();
      return;
    }
    if (availableTokens.decrementAndGet() < 0) {
      availableTokens.incrementAndGet();
      return;
    }
    try {
      ensureConnected();
      MqttMessage message = new MqttMessage(payload);
      message.setQos(properties.getQos());
      message.setRetained(retain);
      mqttClient.publish(topic, message);
      consecutiveFailures.set(0);
    } catch (Exception e) {
      int failures = consecutiveFailures.incrementAndGet();
      log.error("NMI MQTT publish failed for topic {}: {}", topic, e.getMessage());
      if (failures >= Math.max(properties.getCircuitBreakerFailureThreshold(), 1)) {
        circuitOpen = true;
        log.error("Opening NMI circuit breaker after {} failures", failures);
      }
      throw new RuntimeException(e);
    }
  }

  public synchronized void resetCircuitBreaker() {
    circuitOpen = false;
    consecutiveFailures.set(0);
  }

  private void ensureConnected() throws MqttException {
    if (mqttClient != null && mqttClient.isConnected()) {
      return;
    }
    String serverUri = toPahoConnectionUri(properties.getBrokerUri());
    String clientId = properties.getClientId() == null || properties.getClientId().isBlank()
        ? "jpo-mec-deposit-nmi-" + UUID.randomUUID() : properties.getClientId();
    mqttClient = new MqttClient(serverUri, clientId);
    MqttConnectOptions options = new MqttConnectOptions();
    options.setServerURIs(new String[] {serverUri});
    options.setCleanSession(true);
    options.setAutomaticReconnect(true);
    options.setConnectionTimeout(10);
    options.setKeepAliveInterval(30);
    mqttClient.connect(options);
  }

  /**
   * Eclipse Paho MQTT v3 expects {@code tcp://} / {@code ssl://}, not {@code mqtt://} /
   * {@code mqtts://} (those schemes are rejected with "no NetworkModule installed").
   */
  public static String toPahoConnectionUri(String brokerUri) {
    if (brokerUri == null) {
      return null;
    }
    if (brokerUri.isBlank()) {
      return brokerUri.trim();
    }
    try {
      URI uri = URI.create(brokerUri.trim());
      String scheme = uri.getScheme();
      if (scheme == null) {
        return brokerUri.trim();
      }
      String lower = scheme.toLowerCase(Locale.ROOT);
      if ("mqtt".equals(lower)) {
        return replaceScheme(uri, "tcp");
      }
      if ("mqtts".equals(lower)) {
        return replaceScheme(uri, "ssl");
      }
      return uri.toASCIIString();
    } catch (IllegalArgumentException e) {
      return brokerUri.trim();
    }
  }

  private static String replaceScheme(URI uri, String newScheme) {
    try {
      return new URI(newScheme, uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(),
          uri.getQuery(), uri.getFragment()).toASCIIString();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid MQTT broker URI: " + uri, e);
    }
  }
}
