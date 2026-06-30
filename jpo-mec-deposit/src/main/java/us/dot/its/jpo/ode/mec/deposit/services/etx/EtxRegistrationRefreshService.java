package us.dot.its.jpo.ode.mec.deposit.services.etx;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import us.dot.its.jpo.ode.mec.deposit.config.mqtt.EtxMqttBrokerConfig;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;

/**
 * Service for tracking MQTT publish failures and triggering ETX registration refresh when failures
 * exceed a threshold. This helps recover from scenarios where the MQTT connection or registration
 * has become invalid.
 */
@Slf4j
@Service
@ConditionalOnProperty(value = {"mec-deposit.etx.enabled"}, havingValue = "true")
public class EtxRegistrationRefreshService {
  private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
  private final ReentrantLock refreshLock = new ReentrantLock();
  private final int failureThreshold;
  private final EtxMqttBrokerConfig etxMqttConfig;
  private volatile long lastRefreshAttempt = 0;
  private static final long MIN_REFRESH_INTERVAL_MS = 60000;

  /**
   * Constructs the registration refresh service.
   *
   * @param mqttProperties MQTT configuration properties
   * @param etxMqttConfig ETX MQTT configuration for triggering refresh
   */
  public EtxRegistrationRefreshService(EtxMqttProperties mqttProperties,
      EtxMqttBrokerConfig etxMqttConfig) {
    this.etxMqttConfig = etxMqttConfig;
    // Default threshold: 10 consecutive failures, configurable via properties
    this.failureThreshold = mqttProperties.getRegistrationRefreshFailureThreshold() > 0
        ? mqttProperties.getRegistrationRefreshFailureThreshold()
        : 10;
    log.info("ETX registration refresh service initialized with failure threshold: {}",
        this.failureThreshold);
  }

  /**
   * Records a successful MQTT publish, resetting the failure counter.
   */
  public void recordSuccess() {
    int failures = consecutiveFailures.getAndSet(0);
    if (failures > 0) {
      log.info("MQTT publish succeeded, resetting failure counter (was at {} failures)", failures);
    }
  }

  /**
   * Records a failed MQTT publish and triggers registration refresh if threshold is exceeded.
   *
   * @return true if a registration refresh was triggered, false otherwise
   */
  public boolean recordFailure() {
    int failures = consecutiveFailures.incrementAndGet();
    log.warn("MQTT publish failure recorded (consecutive failures: {}/{})", failures,
        failureThreshold);

    if (failures >= failureThreshold) {
      return attemptRefresh();
    }
    return false;
  }

  /**
   * Attempts to refresh the ETX registration if enough time has passed since the last attempt.
   *
   * @return true if refresh was attempted, false if skipped due to rate limiting
   */
  private boolean attemptRefresh() {
    long now = System.currentTimeMillis();
    long timeSinceLastRefresh = now - lastRefreshAttempt;

    if (timeSinceLastRefresh < MIN_REFRESH_INTERVAL_MS) {
      log.warn(
          "Skipping registration refresh - last refresh attempt was {} ms ago (minimum interval: {} ms)",
          timeSinceLastRefresh, MIN_REFRESH_INTERVAL_MS);
      return false;
    }

    // Use lock to prevent concurrent refresh attempts
    if (!refreshLock.tryLock()) {
      log.warn("Registration refresh already in progress, skipping");
      return false;
    }

    try {
      lastRefreshAttempt = now;
      log.warn(
          "MQTT publish failures exceeded threshold ({}), attempting to refresh ETX registration",
          failureThreshold);

      boolean refreshed = etxMqttConfig.refreshRegistration();
      if (refreshed) {
        log.info("ETX registration refresh completed successfully, resetting failure counter");
        consecutiveFailures.set(0);
        return true;
      } else {
        log.error("ETX registration refresh failed");
        return false;
      }
    } catch (Exception e) {
      log.error("Exception during ETX registration refresh", e);
      return false;
    } finally {
      refreshLock.unlock();
    }
  }

  /**
   * Gets the current number of consecutive failures.
   *
   * @return Current failure count
   */
  public int getConsecutiveFailures() {
    return consecutiveFailures.get();
  }

  /**
   * Manually resets the failure counter. Useful for testing or manual intervention.
   */
  public void resetFailureCounter() {
    int failures = consecutiveFailures.getAndSet(0);
    if (failures > 0) {
      log.info("Failure counter manually reset (was at {} failures)", failures);
    }
  }
}

