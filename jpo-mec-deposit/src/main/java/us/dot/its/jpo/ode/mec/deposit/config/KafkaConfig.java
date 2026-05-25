package us.dot.its.jpo.ode.mec.deposit.config;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

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

  @Value("${spring.kafka.consumer.max-poll-records:10}")
  private int maxPollRecords;

  @Value("${KAFKA_MAX_POLL_INTERVAL_MS:600000}")
  private int maxPollIntervalMs;

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
    props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
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
    props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
    return new DefaultKafkaConsumerFactory<>(props);
  }

  /**
   * Rebalance listener that seeks all assigned partitions to the end, so depositors always
   * start from the most recent message on startup rather than replaying a backlog.
   *
   * seekToEnd() is lazy — it only updates the fetch position, not the committed offset stored in
   * Kafka. Without committing, Kafka reports lag = (end offset - old committed offset), which
   * grows indefinitely even though the consumer is reading fresh messages. Calling commitSync()
   * with the resolved end positions immediately closes that gap.
   */
  private static ConsumerAwareRebalanceListener seekToEndOnAssignment() {
    return new ConsumerAwareRebalanceListener() {
      @Override
      public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) {
          return;
        }
        consumer.seekToEnd(partitions);
        Map<TopicPartition, OffsetAndMetadata> endOffsets = partitions.stream()
            .collect(Collectors.toMap(tp -> tp, tp -> new OffsetAndMetadata(consumer.position(tp))));
        consumer.commitSync(endOffsets);
      }
    };
  }

  /**
   * Container factory for depositor listeners. Seeks to the latest offset on every partition
   * assignment so the application processes only fresh messages after a restart.
   *
   * @return ConcurrentKafkaListenerContainerFactory for String messages
   */
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(stringConsumerFactory());
    factory.setConcurrency(concurrency);
    factory.setContainerCustomizer(container ->
        container.getContainerProperties().setConsumerRebalanceListener(seekToEndOnAssignment()));
    return factory;
  }

  /**
   * Container factory for ByteArray depositor listeners (protobuf). Seeks to the latest offset
   * on every partition assignment.
   *
   * @return ConcurrentKafkaListenerContainerFactory for ByteArray messages
   */
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, byte[]> byteArrayKafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(byteArrayConsumerFactory());
    factory.setConcurrency(concurrency);
    factory.setContainerCustomizer(container ->
        container.getContainerProperties().setConsumerRebalanceListener(seekToEndOnAssignment()));
    return factory;
  }

  /**
   * Container factory for collector listeners that must replay historical messages (e.g.
   * MapRefPointCollector). Does NOT seek to end — respects committed offsets and the
   * auto.offset.reset property declared on each listener.
   *
   * @return ConcurrentKafkaListenerContainerFactory for String messages without seek-to-end
   */
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> collectorKafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(stringConsumerFactory());
    factory.setConcurrency(concurrency);
    return factory;
  }
}
