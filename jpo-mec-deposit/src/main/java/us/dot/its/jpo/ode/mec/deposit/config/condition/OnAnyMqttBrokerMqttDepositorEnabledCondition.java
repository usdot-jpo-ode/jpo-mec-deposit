package us.dot.its.jpo.ode.mec.deposit.config.condition;

import java.util.Map;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Matches when either MQTT broker profile enables the given MQTT depositor. Does not require
 * {@code mec-deposit.etx.enabled}, so NMI-only runs (ETX Paho client and ETX publisher off) can still
 * load Kafka MQTT depositors.
 */
public class OnAnyMqttBrokerMqttDepositorEnabledCondition implements Condition {

  private static final String PREFIX = "mec-deposit.etx.mqtt-brokers.";

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    Map<String, Object> attrs =
        metadata.getAnnotationAttributes(ConditionalOnAnyMqttBrokerMqttDepositor.class.getName());
    if (attrs == null) {
      return false;
    }
    String depositor = (String) attrs.get("value");
    if (depositor == null || depositor.isBlank()) {
      return false;
    }
    Environment env = context.getEnvironment();
    String path = "depositors." + depositor + ".mqtt.enabled";
    boolean etx = truthy(env.getProperty(PREFIX + "etx." + path, "false"));
    boolean nmi = truthy(env.getProperty(PREFIX + "nmi." + path, "false"));
    boolean av = truthy(env.getProperty(PREFIX + "av." + path, "false"));
    return etx || nmi || av;
  }

  private static boolean truthy(String value) {
    if (value == null) {
      return false;
    }
    String v = value.trim();
    return Boolean.parseBoolean(v) || "true".equalsIgnoreCase(v) || "yes".equalsIgnoreCase(v) || "1".equals(v);
  }
}
