package us.dot.its.jpo.ode.mec.deposit.config.mqtt;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.av.mqtt.AvMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.mb.mqtt.MbMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.nmi.mqtt.NmiMqttProperties;

/**
 * Exposes nested MQTT broker settings as beans for constructor injection.
 */
@Configuration
public class MqttBrokerConfiguration {

  @Bean
  public EtxMqttProperties etxMqttProperties(EtxProperties etxProperties) {
    if (etxProperties.getMqttBrokers() == null || etxProperties.getMqttBrokers().getEtx() == null) {
      return new EtxMqttProperties();
    }
    EtxMqttProperties mqtt = etxProperties.getMqttBrokers().getEtx().getMqtt();
    return mqtt != null ? mqtt : new EtxMqttProperties();
  }

  @Bean
  public NmiMqttProperties nmiMqttProperties(EtxProperties etxProperties) {
    if (etxProperties.getMqttBrokers() == null || etxProperties.getMqttBrokers().getNmi() == null) {
      return new NmiMqttProperties();
    }
    NmiMqttProperties mqtt = etxProperties.getMqttBrokers().getNmi().getMqtt();
    return mqtt != null ? mqtt : new NmiMqttProperties();
  }

  @Bean
  public AvMqttProperties avMqttProperties(EtxProperties etxProperties) {
    if (etxProperties.getMqttBrokers() == null || etxProperties.getMqttBrokers().getAv() == null) {
      return new AvMqttProperties();
    }
    AvMqttProperties mqtt = etxProperties.getMqttBrokers().getAv().getMqtt();
    return mqtt != null ? mqtt : new AvMqttProperties();
  }

  @Bean
  public MbMqttProperties mbMqttProperties(EtxProperties etxProperties) {
    if (etxProperties.getMqttBrokers() == null || etxProperties.getMqttBrokers().getMb() == null) {
      return new MbMqttProperties();
    }
    MbMqttProperties mqtt = etxProperties.getMqttBrokers().getMb().getMqtt();
    return mqtt != null ? mqtt : new MbMqttProperties();
  }
}
