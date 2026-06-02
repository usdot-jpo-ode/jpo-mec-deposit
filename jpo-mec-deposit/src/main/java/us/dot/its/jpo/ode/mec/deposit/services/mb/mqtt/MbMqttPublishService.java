package us.dot.its.jpo.ode.mec.deposit.services.mb.mqtt;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
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
import us.dot.its.jpo.ode.mec.deposit.models.mb.mqtt.MbMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.services.nmi.mqtt.NmiMqttPublishService;

/**
 * Dedicated MB MQTT publish service.
 *
 * <p>
 * Identical to {@link NmiMqttPublishService} in structure but connects to the MB broker using
 * username/password credentials.
 */
@Slf4j
@Service
public class MbMqttPublishService {
  private final MbMqttProperties properties;
  private final AtomicInteger availableTokens;
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private final Counter circuitBreakerSkippedCounter;
  private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
  private volatile boolean circuitOpen = false;
  private MqttClient mqttClient;

  /**
   * Single-threaded executor with a bounded queue used to serialize and offload the blocking
   * connect+publish operations so the Kafka listener thread is never held. When the broker is
   * unreachable the queue will fill; the DiscardOldestPolicy drops the oldest pending task rather
   * than accumulating an unbounded backlog.
   */
  private final ExecutorService publishExecutor =
      new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(200),
          new ThreadPoolExecutor.DiscardOldestPolicy());

  /**
   * Creates MB MQTT publish service.
   */
  public MbMqttPublishService(MbMqttProperties properties, MeterRegistry registry) {
    this.properties = properties;
    this.availableTokens = new AtomicInteger(Math.max(properties.getMaxMessagesPerSecond(), 1));
    this.circuitBreakerSkippedCounter = Counter.builder("mec-deposit.mb.mqtt.circuit.skipped")
        .description("Messages skipped while MB circuit breaker is open").register(registry);
    scheduler.scheduleAtFixedRate(
        () -> availableTokens.set(Math.max(properties.getMaxMessagesPerSecond(), 1)), 1, 1,
        TimeUnit.SECONDS);
    scheduler.scheduleAtFixedRate(() -> {
      if (circuitOpen) {
        log.info("MB circuit breaker: half-open probe — resetting to allow retry");
        resetCircuitBreaker();
      }
    }, 60, 60, TimeUnit.SECONDS);
  }

  /**
   * Publishes binary ASN.1 payload to MB broker.
   *
   * <p>
   * Fast-path checks (enabled flag, circuit breaker, rate limit) run on the calling thread. The
   * blocking {@code connect + publish} work is submitted to a bounded single-threaded executor so
   * the Kafka listener thread is never held waiting for a potentially unreachable broker.
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

  /**
   * Resets the MB circuit breaker, allowing publish attempts to resume.
   */
  public void resetCircuitBreaker() {
    circuitOpen = false;
    consecutiveFailures.set(0);
    log.info("MB circuit breaker reset");
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
      log.error("MB MQTT publish failed for topic {}: {}", topic, e.getMessage());
      if (failures >= Math.max(properties.getCircuitBreakerFailureThreshold(), 1)) {
        circuitOpen = true;
        log.error("Opening MB circuit breaker after {} failures", failures);
      }
    }
  }

  private void ensureConnected() throws MqttException {
    if (mqttClient != null && mqttClient.isConnected()) {
      return;
    }
    if (mqttClient != null) {
      try {
        mqttClient.disconnect(0);
      } catch (MqttException ignored) {
        // client may already be disconnected or mid-reconnect
      }
      try {
        mqttClient.close();
      } catch (MqttException ignored) {
        // best-effort cleanup of stale client
      }
    }
    String serverUri = NmiMqttPublishService.toPahoConnectionUri(properties.getBrokerUri());
    String clientId = properties.getClientId() == null || properties.getClientId().isBlank()
        ? "jpo-mec-deposit-mb-" + UUID.randomUUID()
        : properties.getClientId();
    mqttClient = new MqttClient(serverUri, clientId);
    MqttConnectOptions options = new MqttConnectOptions();
    options.setServerURIs(new String[] {serverUri});
    options.setCleanSession(true);
    options.setAutomaticReconnect(true);
    options.setConnectionTimeout(10);
    options.setKeepAliveInterval(30);
    options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
    if (properties.getUsername() != null && !properties.getUsername().isBlank()) {
      options.setUserName(properties.getUsername());
    }
    if (properties.getPassword() != null && !properties.getPassword().isBlank()) {
      options.setPassword(properties.getPassword().toCharArray());
    }
    mqttClient.connect(options);
  }
}
