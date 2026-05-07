package us.dot.its.jpo.ode.mec.deposit.services.av.mqtt;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
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
import us.dot.its.jpo.ode.mec.deposit.models.av.mqtt.AvMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.services.nmi.mqtt.NmiMqttPublishService;

/**
 * Dedicated AV MQTT publish service.
 */
@Slf4j
@Service
public class AvMqttPublishService {
  private final AvMqttProperties properties;
  private final AtomicInteger availableTokens;
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private final Counter circuitBreakerSkippedCounter;
  private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
  private volatile boolean circuitOpen = false;
  private MqttClient mqttClient;

  public AvMqttPublishService(AvMqttProperties properties, MeterRegistry registry) {
    this.properties = properties;
    this.availableTokens = new AtomicInteger(Math.max(properties.getMaxMessagesPerSecond(), 1));
    this.circuitBreakerSkippedCounter = Counter.builder("mec-deposit.av.mqtt.circuit.skipped")
        .description("Messages skipped while AV circuit breaker is open").register(registry);
    scheduler.scheduleAtFixedRate(() -> availableTokens.set(Math.max(properties.getMaxMessagesPerSecond(), 1)),
        1, 1, TimeUnit.SECONDS);
  }

  /**
   * Publishes binary ASN.1 payload to AV broker.
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
      log.error("AV MQTT publish failed for topic {}: {}", topic, e.getMessage());
      if (failures >= Math.max(properties.getCircuitBreakerFailureThreshold(), 1)) {
        circuitOpen = true;
        log.error("Opening AV circuit breaker after {} failures", failures);
      }
      throw new RuntimeException(e);
    }
  }

  private void ensureConnected() throws MqttException {
    if (mqttClient != null && mqttClient.isConnected()) {
      return;
    }
    String serverUri = NmiMqttPublishService.toPahoConnectionUri(properties.getBrokerUri());
    String clientId = properties.getClientId() == null || properties.getClientId().isBlank()
        ? "jpo-mec-deposit-av-" + UUID.randomUUID() : properties.getClientId();
    mqttClient = new MqttClient(serverUri, clientId);
    MqttConnectOptions options = new MqttConnectOptions();
    options.setServerURIs(new String[] {serverUri});
    options.setCleanSession(true);
    options.setAutomaticReconnect(true);
    options.setConnectionTimeout(10);
    options.setKeepAliveInterval(30);
    mqttClient.connect(options);
  }
}
