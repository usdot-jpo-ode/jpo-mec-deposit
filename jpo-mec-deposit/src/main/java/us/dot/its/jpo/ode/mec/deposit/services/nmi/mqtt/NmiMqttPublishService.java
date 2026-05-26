package us.dot.its.jpo.ode.mec.deposit.services.nmi.mqtt;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
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
   * Single-threaded executor with a bounded queue used to serialize and offload the blocking
   * connect+publish operations so the Kafka listener thread is never held.  When the broker is
   * unreachable the queue will fill; the DiscardOldestPolicy drops the oldest pending task rather
   * than accumulating an unbounded backlog.
   */
  private final ExecutorService publishExecutor = new ThreadPoolExecutor(
      1, 1, 0L, TimeUnit.MILLISECONDS,
      new ArrayBlockingQueue<>(200),
      new ThreadPoolExecutor.DiscardOldestPolicy());

  /**
   * Creates NMI MQTT publish service.
   */
  public NmiMqttPublishService(NmiMqttProperties properties, MeterRegistry registry) {
    this.properties = properties;
    this.availableTokens = new AtomicInteger(Math.max(properties.getMaxMessagesPerSecond(), 1));
    this.circuitBreakerSkippedCounter = Counter.builder("mec-deposit.nmi.mqtt.circuit.skipped")
        .description("Messages skipped while NMI circuit breaker is open").register(registry);
    scheduler.scheduleAtFixedRate(
        () -> availableTokens.set(Math.max(properties.getMaxMessagesPerSecond(), 1)), 1, 1,
        TimeUnit.SECONDS);
  }

  /**
   * Publishes binary ASN.1 payload to NMI broker.
   *
   * <p>Fast-path checks (enabled flag, circuit breaker, rate limit) run on the calling thread.
   * The blocking {@code connect + publish} work is submitted to a bounded single-threaded executor
   * so the Kafka listener thread is never held waiting for a potentially unreachable broker.
   */
  public void publishAsn1Bytes(String topic, byte[] payload, boolean retain) {
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
    publishExecutor.submit(() -> doPublish(topic, payload, retain));
  }

  public void resetCircuitBreaker() {
    circuitOpen = false;
    consecutiveFailures.set(0);
  }

  private synchronized void doPublish(String topic, byte[] payload, boolean retain) {
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
    }
  }

  private void ensureConnected() throws MqttException {
    if (mqttClient != null && mqttClient.isConnected()) {
      return;
    }
    String serverUri = toPahoConnectionUri(properties.getBrokerUri());
    String clientId = properties.getClientId() == null || properties.getClientId().isBlank()
        ? "jpo-mec-deposit-nmi-" + UUID.randomUUID()
        : properties.getClientId();
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
      return uri.toASCIIString();
    } catch (IllegalArgumentException e) {
      return brokerUri.trim();
    }
  }
}
