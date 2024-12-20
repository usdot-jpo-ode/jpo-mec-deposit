package us.dot.its.jpo.ode.mec.deposit.imp.mqtt;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.springframework.integration.mqtt.support.MqttHeaders;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ImpMqttService {
    private final MessageChannel mqttOutboundChannel;
    private final Executor executor;

    public ImpMqttService(MessageChannel mqttOutboundChannel) {
        this.mqttOutboundChannel = mqttOutboundChannel;

        // Create a dedicated thread pool for MQTT
        // publishing
        this.executor = new ThreadPoolExecutor(4, // core pool size
                8, // max pool size
                60L, TimeUnit.SECONDS, // thread keep alive time
                new LinkedBlockingQueue<>(1000), // queue capacity
                new ThreadPoolExecutor.CallerRunsPolicy() // rejection policy
        );
    }

    @Retryable(value = {
            Exception.class }, maxAttempts = 3, backoff = @Backoff(delay = 100, multiplier = 2))
    public CompletableFuture<Void> publishAsn1Bytes(String topic, byte[] asn1Bytes,
            boolean retain) {
        return CompletableFuture.runAsync(() -> {
            try {
                Message<byte[]> message = MessageBuilder.withPayload(asn1Bytes)
                        .setHeader(MqttHeaders.TOPIC, topic).setHeader(MqttHeaders.RETAINED, retain)
                        .setHeader(MqttHeaders.QOS, 0) // Explicitly set QoS to
                                                       // 0 for fastest delivery
                        .build();

                boolean sent = mqttOutboundChannel.send(message, 1000); // Add timeout
                if (!sent) {
                    throw new RuntimeException("Failed to send message to MQTT channel");
                }
                log.debug("Successfully published message to topic: {}", topic);
            } catch (Exception e) {
                log.error("Error publishing message to topic {}: {}", topic, e.getMessage());
                throw e;
            }
        }, executor); // Use our custom executor
    }
}