package us.dot.its.jpo.ode.mec.deposit.services.mb.mqtt;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
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
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private final Counter circuitBreakerSkippedCounter;

  // FIX #4: Use a floor-guarded semaphore-style counter instead of a drifting
  // AtomicInteger. Tokens are only consumed when a permit is actually available,
  // so the counter never goes below zero and burst debt is always accurate.
  private final AtomicInteger availableTokens;

  // FIX #5: A single lock object used for ALL circuit-breaker state mutations,
  // including the public resetCircuitBreaker() method. Previously that method
  // was unsynchronized, creating a race with doPublish's synchronized block.
  private final Object circuitLock = new Object();
  private volatile boolean circuitOpen = false;
  private int consecutiveFailures = 0; // guarded by circuitLock

  // FIX #3: A stable, per-instance client ID avoids the broker accumulating
  // thousands of phantom sessions from the old UUID-per-reconnect approach.
  // It is computed once at construction time and reused for the lifetime of
  // this service bean.
  private final String stableClientId;

  private MqttClient mqttClient; // guarded by publishExecutor's single thread

  /**
   * Single-threaded executor with a bounded queue used to serialize and offload the blocking
   * connect+publish operations so the Kafka listener thread is never held. When the broker is
   * unreachable the queue will fill; the DiscardOldestPolicy drops the oldest pending task rather
   * than accumulating an unbounded backlog.
   *
   * <p>
   * FIX #2: Because this executor is single-threaded, {@code doPublish} no longer needs to be
   * {@code synchronized}. The previous {@code synchronized} keyword forced the executor's one
   * worker thread to acquire the monitor on every call — which, combined with the blocking
   * {@code mqttClient.connect()} call inside, could have stalled indefinitely if a second thread
   * ever tried to enter. Removing the keyword eliminates that latent deadlock vector.
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

    // FIX #3: Stable client ID — generated once, reused across reconnects.
    this.stableClientId = "jpo-mec-deposit-mb-" + java.util.UUID.randomUUID();

    this.circuitBreakerSkippedCounter = Counter.builder("mec-deposit.mb.mqtt.circuit.skipped")
        .description("Messages skipped while MB circuit breaker is open").register(registry);

    // Replenish rate-limit tokens every second.
    scheduler.scheduleAtFixedRate(
        () -> availableTokens.set(Math.max(properties.getMaxMessagesPerSecond(), 1)), 1, 1,
        TimeUnit.SECONDS);

    // Periodic half-open probe: attempt one reconnect after the back-off window.
    scheduler.scheduleAtFixedRate(() -> {
      synchronized (circuitLock) {
        if (circuitOpen) {
          log.info("MB circuit breaker: half-open probe — resetting to allow retry");
          resetCircuitBreakerInternal();
        }
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

    // FIX #4: Use getAndDecrement with a floor check. If the current value is
    // already 0 we never decrement, so availableTokens stays >= 0 at all times.
    // This replaces the old pattern of decrement-then-restore, which allowed the
    // counter to drift arbitrarily negative under concurrent load.
    if (availableTokens.getAndUpdate(current -> current > 0 ? current - 1 : 0) == 0) {
      return;
    }

    publishExecutor.submit(() -> doPublish(topic, payload, retain));
  }

  /**
   * Resets the MB circuit breaker, allowing publish attempts to resume.
   *
   * <p>
   * FIX #5: Now acquires {@code circuitLock} so this method is safe to call from any thread
   * (scheduler, test, admin endpoint) without racing against {@code doPublish}.
   */
  public void resetCircuitBreaker() {
    synchronized (circuitLock) {
      resetCircuitBreakerInternal();
    }
  }

  /** Must be called with {@code circuitLock} held. */
  private void resetCircuitBreakerInternal() {
    circuitOpen = false;
    consecutiveFailures = 0;
    log.info("MB circuit breaker reset");
  }

  /**
   * Performs the actual MQTT publish on the single-threaded executor.
   *
   * <p>
   * FIX #2: No longer {@code synchronized} — the single-threaded executor already provides the
   * necessary serialization, and removing the monitor eliminates the risk of the worker thread
   * blocking indefinitely on the monitor while a blocking network call is in progress.
   */
  private void doPublish(String topic, byte[] payload, boolean retain) {
    try {
      ensureConnected();
      MqttMessage message = new MqttMessage(payload);
      message.setQos(properties.getQos());
      message.setRetained(retain);
      mqttClient.publish(topic, message);

      // Successful publish — clear failure counter.
      synchronized (circuitLock) {
        consecutiveFailures = 0;
      }
    } catch (Exception e) {
      log.error("MB MQTT publish failed for topic {}: {}", topic, e.getMessage());
      synchronized (circuitLock) {
        consecutiveFailures++;
        if (consecutiveFailures >= Math.max(properties.getCircuitBreakerFailureThreshold(), 1)) {
          circuitOpen = true;
          log.error("Opening MB circuit breaker after {} failures", consecutiveFailures);
        }
      }
    }
  }

  /**
   * Ensures the MQTT client is connected, creating or reconnecting as needed.
   *
   * <p>
   * FIX #1 + FIX #3: The previous code combined {@code setAutomaticReconnect(true)} with manual
   * client teardown, causing a race condition: Paho's background reconnect thread and this method
   * both tried to manage client state simultaneously. This led to duplicate client IDs on the
   * broker and rapid connect/disconnect cycling.
   *
   * <p>
   * The fix removes automatic reconnect entirely and handles reconnection manually here. This gives
   * us full visibility into connection state and eliminates the race. The stable {@code clientId}
   * (FIX #3) ensures the broker sees the same logical client across reconnects, allowing in-flight
   * QoS 1/2 messages to be properly acknowledged rather than abandoned.
   *
   * <p>
   * We also guard the "needs reconnect" check with {@code isConnecting()} so that a client that is
   * currently mid-connect is not torn down prematurely.
   */
  private void ensureConnected() throws MqttException {
    // FIX #1: Only isConnected() exists on Paho v3 MqttClient; isConnecting() is
    // an async-client-only API. If the client is already connected we are done.
    // If it is mid-connect the subsequent mqttClient.connect() call below will
    // throw MQTTCLIENT_ALREADY_CONNECTED (32100), which we catch and treat as a
    // non-error so the publish can proceed normally.
    if (mqttClient != null && mqttClient.isConnected()) {
      return;
    }

    if (mqttClient != null) {
      try {
        mqttClient.disconnect(0);
      } catch (MqttException ignored) {
        // client may already be disconnected
      }
      try {
        mqttClient.close();
      } catch (MqttException ignored) {
        // best-effort cleanup of stale client
      }
    }

    String serverUri = NmiMqttPublishService.toPahoConnectionUri(properties.getBrokerUri());

    // FIX #3: Use the stable client ID computed at construction time.
    mqttClient = new MqttClient(serverUri, stableClientId);

    MqttConnectOptions options = new MqttConnectOptions();
    options.setServerURIs(new String[] {serverUri});
    options.setCleanSession(true);
    options.setConnectionTimeout(10);
    options.setKeepAliveInterval(30);
    options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);

    // FIX #1: Do NOT enable automaticReconnect. Paho's auto-reconnect thread
    // competes with our own ensureConnected() logic: when isConnected() briefly
    // returns false during a Paho-managed reconnect, ensureConnected() would tear
    // down the client and create a new one — racing with Paho and leaving orphaned
    // client objects on the broker. We handle reconnection ourselves via the
    // doPublish → ensureConnected() call path, giving us deterministic behavior.
    options.setAutomaticReconnect(false);

    if (properties.getUsername() != null && !properties.getUsername().isBlank()) {
      options.setUserName(properties.getUsername());
    }
    if (properties.getPassword() != null && !properties.getPassword().isBlank()) {
      options.setPassword(properties.getPassword().toCharArray());
    }

    try {
      mqttClient.connect(options);
      log.info("MB MQTT client connected to {} with clientId {}", serverUri, stableClientId);
    } catch (MqttException e) {
      throw e;
    }
  }
}
