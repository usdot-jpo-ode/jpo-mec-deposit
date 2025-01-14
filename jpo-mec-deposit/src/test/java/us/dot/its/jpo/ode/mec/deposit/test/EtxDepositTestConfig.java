package us.dot.its.jpo.ode.mec.deposit.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.mockito.Mockito;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.web.client.RestTemplate;
import us.dot.its.jpo.ode.mec.deposit.config.mqtt.EtxMqttConfig;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxPartnerClient;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxTokenManager;
import us.dot.its.jpo.ode.mec.deposit.services.etx.EtxMqttPublishService;
import org.springframework.messaging.MessageChannel;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;

@TestConfiguration
public class EtxDepositTestConfig {

  @Bean
  @Primary
  public EtxMqttConfig etxMqttConfig() {
    return Mockito.mock(EtxMqttConfig.class);
  }

  @Bean
  @Primary
  public EtxMqttPublishService etxMqttService() {
    return Mockito.mock(EtxMqttPublishService.class);
  }

  @Bean
  @Primary
  public MeterRegistry meterRegistry() {
    return new SimpleMeterRegistry();
  }

  @Bean
  @Primary
  public ObjectMapper objectMapper() {
    return new ObjectMapper();
  }

  @Bean
  @Primary
  public RestTemplate restTemplate() {
    return Mockito.mock(RestTemplate.class);
  }

  @Bean
  @Primary
  public EtxPartnerClient etxApi() {
    return Mockito.mock(EtxPartnerClient.class);
  }

  @Bean
  @Primary
  public EtxTokenManager etxTokenManager() {
    return Mockito.mock(EtxTokenManager.class);
  }

  @Bean
  @Primary
  public KafkaTemplate<String, String> kafkaTemplate() {
    return Mockito.mock(KafkaTemplate.class);
  }

  @Bean
  @Primary
  public ConsumerFactory<String, String> consumerFactory() {
    return Mockito.mock(ConsumerFactory.class);
  }

  @Bean
  @Primary
  public ProducerFactory<String, String> producerFactory() {
    return Mockito.mock(ProducerFactory.class);
  }

  // @Bean
  // @Primary
  // public MessageChannel mqttOutboundChannel() {
  // return Mockito.mock(MessageChannel.class);
  // }

  // @Bean
  // @Primary
  // public EtxMqttProperties etxMqttProperties() {
  // EtxMqttProperties properties = new EtxMqttProperties();
  // properties.setMaxMessagesPerSecond(100);
  // return properties;
  // }
}
