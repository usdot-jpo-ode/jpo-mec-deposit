package us.dot.its.jpo.ode.mec.deposit.services.mqtt;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.BrokerPublishPayload;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttBrokerTarget;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttFanoutPublishResult;

/**
 * Coordinates multi-broker MQTT fanout publishing with per-target isolation.
 *
 * <p>This service is broker-agnostic. The set of active targets is resolved externally (see
 * {@code MqttBrokerConfiguration#mqttActiveTargets}) and injected here, keeping ETX-specific
 * routing logic out of this class.
 */
@Service
@Slf4j
public class MultiBrokerPublishService {
  private final Map<MqttBrokerTarget, BrokerPublisher> publishers = new EnumMap<>(MqttBrokerTarget.class);
  private final Set<MqttBrokerTarget> activeTargets;
  private final MeterRegistry meterRegistry;

  /**
   * Creates a broker fanout service.
   *
   * @param brokerPublishers all registered broker publisher beans
   * @param activeTargets    the pre-resolved set of targets that should receive publishes
   * @param meterRegistry    metrics registry
   */
  public MultiBrokerPublishService(List<BrokerPublisher> brokerPublishers,
      Set<MqttBrokerTarget> activeTargets, MeterRegistry meterRegistry) {
    for (BrokerPublisher publisher : brokerPublishers) {
      publishers.put(publisher.target(), publisher);
    }
    this.activeTargets = activeTargets;
    this.meterRegistry = meterRegistry;
  }

  /**
   * Publishes each configured broker payload with per-target isolation.
   *
   * @return brokers that published successfully and the union of topics written to those brokers
   */
  public MqttFanoutPublishResult publish(Map<MqttBrokerTarget, BrokerPublishPayload> payloads,
      boolean retain) {
    LinkedHashSet<String> successfulBrokers = new LinkedHashSet<>();
    LinkedHashSet<String> publishedTopics = new LinkedHashSet<>();
    for (MqttBrokerTarget target : activeTargets) {
      BrokerPublishPayload payload = payloads.get(target);
      if (payload == null || payload.getTopics() == null || payload.getTopics().isEmpty()) {
        continue;
      }
      BrokerPublisher publisher = publishers.get(target);
      if (publisher == null) {
        continue;
      }
      try {
        publisher.publish(payload, retain);
        counter("success", target).increment();
        successfulBrokers.add(target.name());
        publishedTopics.addAll(payload.getTopics());
      } catch (Exception e) {
        counter("failure", target).increment();
        log.error("MQTT publish failed for broker target {} with {} topics: {}", target,
            payload.getTopics().size(), e.getMessage(), e);
        if (activeTargets.size() == 1) {
          throw e;
        }
      }
    }
    return new MqttFanoutPublishResult(successfulBrokers, publishedTopics);
  }

  /**
   * Returns whether target is active for publishing.
   */
  public boolean isTargetActive(MqttBrokerTarget target) {
    return activeTargets.contains(target);
  }

  /**
   * Union of all topic strings in the payload map (for metrics when no publish succeeded but
   * topics were computed).
   */
  public static Set<String> unionPayloadTopics(Map<MqttBrokerTarget, BrokerPublishPayload> payloads) {
    if (payloads == null || payloads.isEmpty()) {
      return Set.of();
    }
    return payloads.values().stream()
        .filter(Objects::nonNull)
        .map(BrokerPublishPayload::getTopics)
        .filter(Objects::nonNull)
        .flatMap(Set::stream)
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private Counter counter(String outcome, MqttBrokerTarget target) {
    return Counter.builder("mec-deposit.mqtt.publish")
        .tag("broker", target.name().toLowerCase())
        .tag("outcome", outcome)
        .register(meterRegistry);
  }
}
