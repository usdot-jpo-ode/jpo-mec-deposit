package us.dot.its.jpo.ode.mec.deposit.config.condition;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Conditional;

/**
 * Enables a bean when the MQTT depositor kind (e.g. {@code bsm}) is enabled on at least one MQTT
 * broker profile under {@code mec-deposit.etx.mqtt-brokers} (ETX or NMI). Does not require
 * {@code mec-deposit.etx.enabled}.
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Conditional(OnAnyMqttBrokerMqttDepositorEnabledCondition.class)
public @interface ConditionalOnAnyMqttBrokerMqttDepositor {

  /**
   * Depositor key matching YAML nodes under {@code depositors} (e.g. {@code bsm}, {@code spat}).
   */
  String value();
}
