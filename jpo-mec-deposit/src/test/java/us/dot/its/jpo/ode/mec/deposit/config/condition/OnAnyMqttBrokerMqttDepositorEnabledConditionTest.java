package us.dot.its.jpo.ode.mec.deposit.config.condition;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;

@ExtendWith(MockitoExtension.class)
class OnAnyMqttBrokerMqttDepositorEnabledConditionTest {

  @Mock
  private ConditionContext context;

  @Mock
  private Environment environment;

  @Mock
  private AnnotatedTypeMetadata metadata;

  private final OnAnyMqttBrokerMqttDepositorEnabledCondition condition =
      new OnAnyMqttBrokerMqttDepositorEnabledCondition();

  @Test
  void matchesWhenSharedPsmEnvTrueDespiteBlankBrokerProperty() {
    when(context.getEnvironment()).thenReturn(environment);
    when(metadata.getAnnotationAttributes(ConditionalOnAnyMqttBrokerMqttDepositor.class.getName()))
        .thenReturn(java.util.Map.of("value", "psm"));
    when(environment.getProperty("mec-deposit.etx.mqtt-brokers.etx.depositors.psm.mqtt.enabled"))
        .thenReturn("");
    when(environment.getProperty("mec-deposit.etx.mqtt-brokers.nmi.depositors.psm.mqtt.enabled"))
        .thenReturn("");
    when(environment.getProperty("mec-deposit.etx.mqtt-brokers.av.depositors.psm.mqtt.enabled"))
        .thenReturn("");
    when(environment.getProperty("ETX_DEPOSITORS_PSM_MQTT_ENABLED")).thenReturn("True");

    assertTrue(condition.matches(context, metadata));
  }

  @Test
  void doesNotMatchWhenSharedAndBrokerPropertiesDisabled() {
    when(context.getEnvironment()).thenReturn(environment);
    when(metadata.getAnnotationAttributes(ConditionalOnAnyMqttBrokerMqttDepositor.class.getName()))
        .thenReturn(java.util.Map.of("value", "psm"));
    when(environment.getProperty("mec-deposit.etx.mqtt-brokers.etx.depositors.psm.mqtt.enabled"))
        .thenReturn("false");
    when(environment.getProperty("mec-deposit.etx.mqtt-brokers.nmi.depositors.psm.mqtt.enabled"))
        .thenReturn("false");
    when(environment.getProperty("mec-deposit.etx.mqtt-brokers.av.depositors.psm.mqtt.enabled"))
        .thenReturn("false");

    assertFalse(condition.matches(context, metadata));
  }
}
