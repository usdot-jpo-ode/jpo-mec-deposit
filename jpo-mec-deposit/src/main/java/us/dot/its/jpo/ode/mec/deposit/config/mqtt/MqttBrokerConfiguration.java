package us.dot.its.jpo.ode.mec.deposit.config.mqtt;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.av.mqtt.AvMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttBrokerTarget;
import us.dot.its.jpo.ode.mec.deposit.models.mb.mqtt.MbMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.nmi.mqtt.NmiMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.services.mqtt.BrokerPublisher;

/**
 * Exposes nested MQTT broker settings as beans for constructor injection.
 */
@Configuration
public class MqttBrokerConfiguration {

  /**
   * Resolves the set of active {@link MqttBrokerTarget}s from ETX MQTT routing configuration and
   * the set of registered {@link BrokerPublisher} beans.
   *
   * <p>When multi-broker mode is enabled the configured target list is used directly. When
   * single-broker mode is active the ETX {@code brokerType} property maps to the corresponding
   * target. In both cases the result is intersected with registered publishers — a target without a
   * publisher is silently dropped. If the intersection is empty all registered publishers are
   * treated as active (safe fallback).
   */
  @Bean
  public Set<MqttBrokerTarget> mqttActiveTargets(EtxMqttProperties mqttProperties,
      List<BrokerPublisher> brokerPublishers) {
    Set<MqttBrokerTarget> registeredTargets =
        brokerPublishers.stream().map(BrokerPublisher::target).collect(Collectors.toSet());

    Set<MqttBrokerTarget> configured;
    if (mqttProperties.isMultiBrokerEnabled()) {
      configured = mqttProperties.getMultiBrokerTargets().stream().collect(Collectors.toSet());
    } else {
      MqttBrokerTarget brokerType =
          mqttProperties.getBrokerType() != null ? mqttProperties.getBrokerType()
              : MqttBrokerTarget.ETX;
      configured = Set.of(brokerType);
    }

    EnumSet<MqttBrokerTarget> resolved = configured.stream()
        .filter(registeredTargets::contains)
        .collect(Collectors.toCollection(() -> EnumSet.noneOf(MqttBrokerTarget.class)));
    if (resolved.isEmpty()) {
      resolved.addAll(registeredTargets);
    }
    return Collections.unmodifiableSet(resolved);
  }

  /** Exposes the ETX-broker MQTT properties as a bean. */
  @Bean
  public EtxMqttProperties etxMqttProperties(EtxProperties etxProperties) {
    if (etxProperties.getMqttBrokers() == null || etxProperties.getMqttBrokers().getEtx() == null) {
      return new EtxMqttProperties();
    }
    EtxMqttProperties mqtt = etxProperties.getMqttBrokers().getEtx().getMqtt();
    return mqtt != null ? mqtt : new EtxMqttProperties();
  }

  /** Exposes the NMI-broker MQTT properties as a bean. */
  @Bean
  public NmiMqttProperties nmiMqttProperties(EtxProperties etxProperties) {
    if (etxProperties.getMqttBrokers() == null || etxProperties.getMqttBrokers().getNmi() == null) {
      return new NmiMqttProperties();
    }
    NmiMqttProperties mqtt = etxProperties.getMqttBrokers().getNmi().getMqtt();
    return mqtt != null ? mqtt : new NmiMqttProperties();
  }

  /** Exposes the AV-broker MQTT properties as a bean. */
  @Bean
  public AvMqttProperties avMqttProperties(EtxProperties etxProperties) {
    if (etxProperties.getMqttBrokers() == null || etxProperties.getMqttBrokers().getAv() == null) {
      return new AvMqttProperties();
    }
    AvMqttProperties mqtt = etxProperties.getMqttBrokers().getAv().getMqtt();
    return mqtt != null ? mqtt : new AvMqttProperties();
  }

  /** Exposes the MB-broker MQTT properties as a bean. */
  @Bean
  public MbMqttProperties mbMqttProperties(EtxProperties etxProperties) {
    if (etxProperties.getMqttBrokers() == null || etxProperties.getMqttBrokers().getMb() == null) {
      return new MbMqttProperties();
    }
    MbMqttProperties mqtt = etxProperties.getMqttBrokers().getMb().getMqtt();
    return mqtt != null ? mqtt : new MbMqttProperties();
  }
}
