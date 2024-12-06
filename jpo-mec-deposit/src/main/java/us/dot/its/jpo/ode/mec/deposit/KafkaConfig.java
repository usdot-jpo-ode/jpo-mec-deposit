// package us.dot.its.jpo.ode.mec.deposit;

// import java.util.ArrayList;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
// import java.util.Properties;
// import java.util.Set;
// import java.util.concurrent.ExecutionException;

// import org.apache.kafka.clients.admin.Admin;
// import org.apache.kafka.clients.admin.ListTopicsOptions;
// import org.apache.kafka.clients.admin.ListTopicsResult;
// import org.apache.kafka.clients.admin.NewTopic;
// import org.apache.kafka.clients.consumer.ConsumerConfig;
// import org.apache.kafka.common.KafkaFuture;
// import org.apache.kafka.common.serialization.StringSerializer;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.boot.context.properties.ConfigurationProperties;
// import org.springframework.context.annotation.Bean;
// import org.springframework.context.annotation.Configuration;
// import org.springframework.kafka.annotation.EnableKafka;
// import
// org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
// import org.springframework.kafka.config.TopicBuilder;
// import org.springframework.kafka.core.ConsumerFactory;
// import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
// import org.springframework.kafka.core.DefaultKafkaProducerFactory;
// import org.springframework.kafka.core.KafkaAdmin;
// import org.springframework.kafka.core.KafkaAdmin.NewTopics;
// import org.springframework.kafka.core.KafkaTemplate;
// import org.springframework.kafka.core.ProducerFactory;
// import org.springframework.stereotype.Component;
// import org.apache.kafka.clients.producer.ProducerConfig;

// // @EnableKafka
// @Configuration
// public class KafkaConfig {

// @Autowired
// private DepositorProperties properties;

// @Bean
// public ConsumerFactory<String, String> consumerFactory() {
// Map<String, Object> props = new HashMap<>();
// props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
// properties.getKafkaBrokers());
// props.put(ConsumerConfig.GROUP_ID_CONFIG, "jpo-s3-depositor");
// props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, JsonSerdes);
// props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
// StringDeserializer.class);
// return new DefaultKafkaConsumerFactory<>(props);
// }

// @Bean
// public ConcurrentKafkaListenerContainerFactory<String, String>
// kafkaListenerContainerFactory() {

// ConcurrentKafkaListenerContainerFactory<String, String> factory = new
// ConcurrentKafkaListenerContainerFactory<>();
// factory.setConsumerFactory(consumerFactory());
// return factory;
// }
// }