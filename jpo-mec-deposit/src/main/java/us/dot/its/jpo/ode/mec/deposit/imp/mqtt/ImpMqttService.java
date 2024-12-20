package us.dot.its.jpo.ode.mec.deposit.imp.mqtt;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.springframework.integration.mqtt.support.MqttHeaders;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

@Slf4j
@Service
public class ImpMqttService {
    private final MessageChannel mqttOutboundChannel;
    private final Executor executor;
    private final AtomicInteger messageCount = new AtomicInteger(0);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final int maxMessagesPerSecond;
    private final Counter rateLimitSkippedCounter;

    public ImpMqttService(MessageChannel mqttOutboundChannel, ImpMqttProperties mqttProperties,
            MeterRegistry registry) {
        this.mqttOutboundChannel = mqttOutboundChannel;
        this.maxMessagesPerSecond = mqttProperties.getMaxMessagesPerSecond();

        this.rateLimitSkippedCounter = Counter.builder("imp.mqtt.ratelimit.skipped")
                .description("Number of messages skipped due to rate limiting").register(registry);

        // Create a dedicated thread pool for MQTT publishing
        this.executor = new ThreadPoolExecutor(4, 8, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

        // Reset message counter every second
        scheduler.scheduleAtFixedRate(() -> {
            int count = messageCount.getAndSet(0);
            if (count > 0) {
                log.debug("Published {} messages in the last second", count);
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    public CompletableFuture<Void> publishAsn1Bytes(String topic, byte[] asn1Bytes,
            boolean retain) {
        return CompletableFuture.runAsync(() -> {
            try {
                // Check if we've exceeded our rate limit
                if (messageCount.get() >= maxMessagesPerSecond) {
                    rateLimitSkippedCounter.increment();
                    log.warn("Skipping message publish - exceeded rate limit of {}/second",
                            maxMessagesPerSecond);
                    return;
                }

                Message<byte[]> message = MessageBuilder.withPayload(asn1Bytes)
                        .setHeader(MqttHeaders.TOPIC, topic).setHeader(MqttHeaders.RETAINED, retain)
                        .setHeader(MqttHeaders.QOS, 0).build();

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
        }, executor);
    }
}