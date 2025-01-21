package us.dot.its.jpo.ode.mec.deposit.services.etx;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.integration.mqtt.support.MqttHeaders;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.stereotype.Service;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;

/**
 * Service for handling MQTT message publishing with rate limiting and retry capabilities.
 */
@Slf4j
@Service
@ConditionalOnProperty(value = {"mec-deposit.etx.enabled"}, havingValue = "true")
public class EtxMqttPublishService {
  private final MessageChannel mqttOutboundChannel;
  private final Executor executor;
  private final AtomicInteger messageCount = new AtomicInteger(0);
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private final int maxMessagesPerSecond;
  private final Counter rateLimitSkippedCounter;
  private final AtomicInteger currentRate = new AtomicInteger(0);

  /**
   * Constructs an EtxMqttService with the specified parameters.
   *
   * @param mqttOutboundChannel The channel for outbound MQTT messages
   * @param mqttProperties      MQTT configuration properties
   * @param registry            Metrics registry for monitoring
   */
  public EtxMqttPublishService(MessageChannel mqttOutboundChannel, EtxMqttProperties mqttProperties,
                               MeterRegistry registry) {
    this.mqttOutboundChannel = mqttOutboundChannel;
    this.maxMessagesPerSecond = mqttProperties.getMaxMessagesPerSecond();

    this.rateLimitSkippedCounter = Counter.builder("mec-deposit.etx.mqtt.ratelimit.skipped")
        .description("Number of messages skipped due to rate limiting").register(registry);

    // Add gauge metric for current publish rate
    registry.gauge("mec-deposit.etx.mqtt.publish.rate", currentRate);

    // Create a dedicated thread pool for MQTT publishing
    this.executor = new ThreadPoolExecutor(4, 8, 60L, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    // Reset message counter every second and update rate
    scheduler.scheduleAtFixedRate(() -> {
      int count = messageCount.getAndSet(0);
      currentRate.set(count); // Update the current rate
      if (count > 0) {
        log.info("Published {} messages in the last second", count);
      } else {
        log.debug("No messages published in the last second");
      }
    }, 1, 1, TimeUnit.SECONDS);
  }

  /**
   * Publishes ASN.1 encoded bytes to the specified MQTT topic.
   *
   * @param topic     The MQTT topic to publish to
   * @param asn1Bytes The ASN.1 encoded message bytes
   * @param retain    Whether to retain the message on the broker
   */
  public void publishAsn1Bytes(String topic, byte[] asn1Bytes, boolean retain) {
    try {
      // Check if we've exceeded our rate limit
      if (messageCount.get() >= maxMessagesPerSecond) {
        rateLimitSkippedCounter.increment();
        log.warn("Skipping message publish - exceeded rate limit of {} Hz", maxMessagesPerSecond);
        throw new RuntimeException("Message skipped - exceeded rate limit");
      }

      Message<byte[]> message =
          MessageBuilder.withPayload(asn1Bytes).setHeader(MqttHeaders.TOPIC, topic)
              .setHeader(MqttHeaders.RETAINED, retain).setHeader(MqttHeaders.QOS, 0).build();

      boolean sent = mqttOutboundChannel.send(message, 1000);
      if (!sent) {
        throw new RuntimeException("Failed to send message to MQTT channel");
      }

      messageCount.incrementAndGet();
      log.debug("Successfully published message to topic: {}", topic);
    } catch (Exception e) {
      log.error("Error publishing message to topic {}: {}", topic, e.getMessage());
      throw e;
    }
  }
}
