package us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Outcome of a multi-broker MQTT fanout publish: which brokers succeeded and which topics were
 * written (one Kafka metrics record can describe the whole fanout).
 */
public record MqttFanoutPublishResult(Set<String> mqttBrokerTargets, Set<String> publishedTopics) {
  /**
   * Compact canonical constructor that normalises null or empty inputs to immutable empty sets.
   */
  public MqttFanoutPublishResult {
    mqttBrokerTargets = mqttBrokerTargets == null || mqttBrokerTargets.isEmpty()
        ? Set.of()
        : Collections.unmodifiableSet(new LinkedHashSet<>(mqttBrokerTargets));
    publishedTopics = publishedTopics == null || publishedTopics.isEmpty()
        ? Set.of()
        : Collections.unmodifiableSet(new LinkedHashSet<>(publishedTopics));
  }

  public static MqttFanoutPublishResult empty() {
    return new MqttFanoutPublishResult(Set.of(), Set.of());
  }
}
