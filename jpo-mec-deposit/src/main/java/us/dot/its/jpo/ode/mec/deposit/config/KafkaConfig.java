package us.dot.its.jpo.ode.mec.deposit.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for Kafka consumers with support for both String and ByteArray deserializers.
 */
@Configuration
@EnableKafka
public class KafkaConfig {

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServers;

  @Value("${spring.kafka.consumer.group-id}")
  private String groupId;

  @Value("${spring.kafka.consumer.auto-offset-reset:latest}")
  private String autoOffsetReset;

  @Value("${spring.kafka.consumer.enable-auto-commit:true}")
  private boolean enableAutoCommit;

  @Value("${spring.kafka.consumer.auto-commit-interval:50}")
  private int autoCommitInterval;

  @Value("${spring.kafka.consumer.heartbeat-interval:3000}")
  private int heartbeatInterval;

  @Value("${spring.kafka.consumer.fetch-min-size:1}")
  private int fetchMinSize;

  @Value("${spring.kafka.consumer.fetch-max-wait:10}")
  private int fetchMaxWait;

  @Value("${spring.kafka.consumer.max-poll-records:100}")
  private int maxPollRecords;

  @Value("${spring.kafka.listener.concurrency:1}")
  private int concurrency;

  /**
   * Creates a consumer factory for String messages (default for most depositors).
   *
   * @return ConsumerFactory for String messages
   */
  @Bean
  public ConsumerFactory<String, String> stringConsumerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval);
    props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatInterval);
    props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinSize);
    props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWait);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
    return new DefaultKafkaConsumerFactory<>(props);
  }

  /**
   * Creates a consumer factory for ByteArray messages (for protobuf messages).
   *
   * @return ConsumerFactory for ByteArray messages
   */
  @Bean
  public ConsumerFactory<String, byte[]> byteArrayConsumerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval);
    props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatInterval);
    props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinSize);
    props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWait);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
    return new DefaultKafkaConsumerFactory<>(props);
  }

  /**
   * Creates a Kafka listener container factory for String messages (default).
   *
   * @return ConcurrentKafkaListenerContainerFactory for String messages
   */
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(stringConsumerFactory());
    factory.setConcurrency(concurrency);
    return factory;
  }

  /**
   * Creates a Kafka listener container factory for ByteArray messages (protobuf).
   *
   * @return ConcurrentKafkaListenerContainerFactory for ByteArray messages
   */
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, byte[]> byteArrayKafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(byteArrayConsumerFactory());
    factory.setConcurrency(concurrency);
    return factory;
  }
}
