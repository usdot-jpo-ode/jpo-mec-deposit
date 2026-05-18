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
    boolean etx = brokerDepositorEnabled(env, "etx", depositor, path);
    boolean nmi = brokerDepositorEnabled(env, "nmi", depositor, path);
    boolean av = brokerDepositorEnabled(env, "av", depositor, path);
    return etx || nmi || av;
  }

  private static boolean brokerDepositorEnabled(Environment env, String broker, String depositor,
      String path) {
    String value = env.getProperty(PREFIX + broker + "." + path);
    if (!isBlank(value)) {
      return truthy(value);
    }
    return truthy(env.getProperty(sharedDepositorEnvKey(depositor)));
  }

  private static String sharedDepositorEnvKey(String depositor) {
    return "ETX_DEPOSITORS_" + depositor.toUpperCase() + "_MQTT_ENABLED";
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  private static boolean truthy(String value) {
    if (value == null) {
      return false;
    }
    String v = value.trim();
    return Boolean.parseBoolean(v) || "true".equalsIgnoreCase(v) || "yes".equalsIgnoreCase(v) || "1".equals(v);
  }
}
