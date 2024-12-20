// package us.dot.its.jpo.ode.mec.deposit.kafka;

// import java.util.HashMap;
// import java.util.Map;

// import org.apache.kafka.clients.consumer.ConsumerConfig;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.boot.info.BuildProperties;
// import org.springframework.context.annotation.Bean;
// import org.springframework.context.annotation.Configuration;
// import org.springframework.kafka.annotation.EnableKafka;
// import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
// import org.springframework.kafka.core.ConsumerFactory;
// import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
// import org.springframework.kafka.listener.ContainerProperties;

// import us.dot.its.jpo.ode.mec.deposit.DepositorProperties;

// import org.apache.kafka.common.serialization.StringDeserializer;

// @EnableKafka
// @Configuration
// public class KafkaConsumerConfig {
// private final KafkaProperties kafkaProperties;

// public KafkaConsumerConfig(KafkaProperties kafkaProperties) {
// this.kafkaProperties = kafkaProperties;
// }

// @Bean
// public ConsumerFactory<String, String> consumerFactory() {
// Map<String, Object> props = new HashMap<>();
// props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
// props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

// if ("CONFLUENT".equalsIgnoreCase(kafkaProperties.getKafkaType())) {
// props.putAll(kafkaProperties.getConfluent().buildConfluentProperties());
// }

// return new DefaultKafkaConsumerFactory<>(props);
// }

// @Bean
// public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
// ConcurrentKafkaListenerContainerFactory<String, String> factory = new
// ConcurrentKafkaListenerContainerFactory<>();
// factory.setConsumerFactory(consumerFactory());
// return factory;
// }
// }
