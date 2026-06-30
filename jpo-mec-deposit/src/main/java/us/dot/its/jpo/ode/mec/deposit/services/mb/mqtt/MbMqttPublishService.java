package us.dot.its.jpo.ode.mec.deposit.services.mb.mqtt;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.stereotype.Service;
import us.dot.its.jpo.ode.mec.deposit.models.mb.mqtt.MbMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.services.nmi.mqtt.NmiMqttPublishService;

/**
 * Dedicated MB MQTT publish service.
 *
 * <p>Designed for high-throughput (≥1000 Hz) publishing to the MB broker.
 *
 * <p><b>Reconnect strategy:</b> The Paho client is created once at startup and reused for the
 * lifetime of the bean. {@code setAutomaticReconnect(true)} is enabled, and because we never tear
 * down or recreate the client object, there is no race between Paho's internal reconnect thread
 * and our own code. A {@link MqttCallbackExtended} callback handles connection lifecycle events:
 * {@code connectionLost} is logged, and {@code connectComplete} automatically resets the circuit
 * breaker so publishing resumes without operator intervention.
 *
 * <p><b>Publish path at 1000 Hz:</b>
 * <ol>
 *   <li>Fast-path checks (enabled, circuit open, rate limit) execute on the Kafka listener thread
 *       with no blocking.</li>
 *   <li>Accepted messages are submitted to a single-threaded, bounded executor (capacity 200).
 *       Overflow is silently discarded (oldest-first) to prevent backlog accumulation.</li>
 *   <li>{@code doPublish} calls {@code mqttClient.isConnected()} first. If the broker is
 *       unreachable, the call returns immediately and increments the failure counter rather than
 *       blocking on a connection attempt — critical at high publish rates.</li>
 *   <li>After {@code circuitBreakerFailureThreshold} consecutive failures the circuit opens.
 *       Once Paho reconnects, {@code connectComplete} fires and the circuit resets.</li>
 * </ol>
 */
@Slf4j
@Service
public class MbMqttPublishService {

  private static final int STARTUP_RETRY_SECONDS = 30;

  private final MbMqttProperties properties;
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private final Counter circuitBreakerSkippedCounter;
  private final AtomicInteger availableTokens;

  private final Object circuitLock = new Object();
  private volatile boolean circuitOpen = false;
  private int consecutiveFailures = 0; // guarded by circuitLock

  // Stable, predictable client ID taken from configuration (defaulting to a fixed string).
  // Reusing the same ID across reconnects allows the broker to reconcile in-flight QoS 1/2
  // deliveries and avoids accumulating phantom sessions.
  private final String stableClientId;

  // Created once in init(). Paho manages all reconnections internally; this reference is
  // written only during init() and is thereafter read-only from multiple threads.
  private volatile MqttClient mqttClient;

  /**
   * Single-threaded executor with a bounded queue used to serialize and offload the blocking
   * publish operations so the Kafka listener thread is never held. When the broker is
   * unreachable, {@code doPublish} fails fast (no blocking connect), so the executor clears
   * quickly. The DiscardOldestPolicy prevents unbounded backlog growth at high publish rates.
   */
  private final ExecutorService publishExecutor =
      new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(200),
          new ThreadPoolExecutor.DiscardOldestPolicy());

  /**
   * Creates the MB MQTT publish service.
   */
  public MbMqttPublishService(MbMqttProperties properties, MeterRegistry registry) {
    this.properties = properties;
    this.availableTokens = new AtomicInteger(Math.max(properties.getMaxMessagesPerSecond(), 1));

    String configuredId = properties.getClientId();
    this.stableClientId = (configuredId != null && !configuredId.isBlank())
        ? configuredId : "jpo-mec-deposit-mb";

    this.circuitBreakerSkippedCounter = Counter.builder("mec-deposit.mb.mqtt.circuit.skipped")
        .description("Messages skipped while MB circuit breaker is open").register(registry);

    // Replenish rate-limit tokens every second.
    scheduler.scheduleAtFixedRate(
        () -> availableTokens.set(Math.max(properties.getMaxMessagesPerSecond(), 1)),
        1, 1, TimeUnit.SECONDS);
  }

  /**
   * Creates the Paho client and initiates the first connection asynchronously so Spring context
   * startup is never blocked by a slow or unavailable broker.
   *
   * <p>The {@link MqttCallbackExtended} wired here handles all subsequent reconnection lifecycle
   * events for the lifetime of the bean. Paho's {@code automaticReconnect} takes over once the
   * first connection succeeds; if the initial connect fails, the scheduler retries every
   * {@value #STARTUP_RETRY_SECONDS} seconds until it succeeds.
   */
  @PostConstruct
  void init() {
    if (!properties.isEnabled()) {
      log.debug("MB MQTT disabled — skipping client init");
      return;
    }
    try {
      String serverUri = NmiMqttPublishService.toPahoConnectionUri(properties.getBrokerUri());
      mqttClient = new MqttClient(serverUri, stableClientId, new MemoryPersistence());
      mqttClient.setCallback(new MqttCallbackExtended() {
        @Override
        public void connectComplete(boolean reconnect, String serverURI) {
          log.info("MB MQTT {} to {} (clientId={})",
              reconnect ? "reconnected" : "connected", serverURI, stableClientId);
          // Reconnect succeeded — allow publishes to resume immediately.
          synchronized (circuitLock) {
            resetCircuitBreakerInternal();
          }
        }

        @Override
        public void connectionLost(Throwable cause) {
          log.warn("MB MQTT connection lost (clientId={}): {} — Paho will auto-reconnect",
              stableClientId, cause.getMessage());
          // Do NOT open the circuit here. Pending doPublish tasks will detect
          // !isConnected() and increment the failure counter, naturally tripping
          // the circuit after the configured threshold.
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) {
          // Outbound-only client; no subscriptions.
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
          // QoS 0 — no delivery acknowledgement needed.
        }
      });
    } catch (MqttException e) {
      log.error("MB MQTT client creation failed: {} — MB publishing will be unavailable",
          e.getMessage());
      return;
    }

    // Submit initial connect asynchronously; retries handled by attemptConnect().
    scheduler.submit(this::attemptConnect);
  }

  /**
   * Publishes binary ASN.1 payload to the MB broker.
   *
   * <p>Fast-path checks run on the calling thread with no blocking. The actual publish is
   * submitted to the single-threaded executor to avoid holding the Kafka listener thread.
   *
   * @param topic   MQTT topic string
   * @param payload ASN.1 encoded message bytes
   * @param retain  whether the broker should retain the message
   */
  public void publishAsn1Bytes(String topic, byte[] payload, boolean retain) {
    if (!properties.isEnabled()) {
      return;
    }
    if (circuitOpen) {
      circuitBreakerSkippedCounter.increment();
      return;
    }
    if (availableTokens.getAndUpdate(current -> current > 0 ? current - 1 : 0) == 0) {
      return;
    }
    publishExecutor.submit(() -> doPublish(topic, payload, retain));
  }

  /**
   * Resets the MB circuit breaker, allowing publish attempts to resume immediately.
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
   * <p>Fails immediately when not connected — there is no blocking connect attempt here.
   * At ≥1000 Hz, blocking in this method while the broker is down would back-pressure the entire
   * executor queue. Instead we rely on Paho's background reconnect loop, and the circuit breaker
   * trips quickly (within ~{@code circuitBreakerFailureThreshold} messages) so that the overhead
   * of the fast-failing tasks is minimal.
   */
  private void doPublish(String topic, byte[] payload, boolean retain) {
    if (mqttClient == null || !mqttClient.isConnected()) {
      log.warn("MB MQTT client not connected — skipping publish to topic: {}", topic);
      recordFailure();
      return;
    }
    try {
      MqttMessage message = new MqttMessage(payload);
      message.setQos(properties.getQos());
      message.setRetained(retain);
      mqttClient.publish(topic, message);
      synchronized (circuitLock) {
        consecutiveFailures = 0;
      }
    } catch (MqttException e) {
      log.error("MB MQTT publish failed for topic {}: {}", topic, e.getMessage());
      recordFailure();
    }
  }

  private void recordFailure() {
    synchronized (circuitLock) {
      consecutiveFailures++;
      if (!circuitOpen
          && consecutiveFailures >= Math.max(properties.getCircuitBreakerFailureThreshold(), 1)) {
        circuitOpen = true;
        log.warn("Opening MB circuit breaker after {} consecutive failures", consecutiveFailures);
      }
    }
  }

  /**
   * Attempts to connect the Paho client. If the attempt fails, schedules a retry after
   * {@value #STARTUP_RETRY_SECONDS} seconds. Once a connection succeeds, Paho's
   * {@code automaticReconnect} takes over and no further manual retries are needed.
   */
  private void attemptConnect() {
    if (mqttClient == null || mqttClient.isConnected()) {
      return;
    }
    try {
      String serverUri = NmiMqttPublishService.toPahoConnectionUri(properties.getBrokerUri());
      mqttClient.connect(buildConnectOptions(serverUri));
      // connectComplete callback fires and resets the circuit breaker.
    } catch (MqttException e) {
      log.warn("MB MQTT connection attempt failed (clientId={}): {} — retrying in {}s",
          stableClientId, e.getMessage(), STARTUP_RETRY_SECONDS);
      scheduler.schedule(this::attemptConnect, STARTUP_RETRY_SECONDS, TimeUnit.SECONDS);
    }
  }

  private MqttConnectOptions buildConnectOptions(String serverUri) {
    MqttConnectOptions options = new MqttConnectOptions();
    options.setServerURIs(new String[] {serverUri});
    options.setCleanSession(true);
    options.setConnectionTimeout(10);
    options.setKeepAliveInterval(30);
    options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
    // Enabled here because the client is never torn down or recreated, so there is no
    // race between Paho's reconnect thread and our own code. The connectComplete callback
    // resets the circuit breaker each time Paho successfully (re)connects.
    options.setAutomaticReconnect(true);
    if (properties.getUsername() != null && !properties.getUsername().isBlank()) {
      options.setUserName(properties.getUsername());
    }
    if (properties.getPassword() != null && !properties.getPassword().isBlank()) {
      options.setPassword(properties.getPassword().toCharArray());
    }
    return options;
  }
}
