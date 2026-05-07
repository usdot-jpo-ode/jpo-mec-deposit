package us.dot.its.jpo.ode.mec.deposit.services.etx;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxPartnerApiProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxUtil;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.RegistrationConfiguration;

/**
 * Service for handling MQTT message publishing with rate limiting and retry capabilities.
 */
@Slf4j
@Service
@ConditionalOnProperty(value = {"mec-deposit.etx.enabled"}, havingValue = "true")
public class EtxMqttPublishService {
  private final MessageChannel mqttOutboundChannel;
  private final EtxPartnerApiProperties partnerApiProperties;
  private final AtomicInteger messageCount = new AtomicInteger(0);
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private final int maxMessagesPerSecond;
  private final Counter rateLimitSkippedCounter;
  private final AtomicInteger currentRate = new AtomicInteger(0);
  private final AtomicInteger availableTokens;
  private final boolean requireSessionId;

  /**
   * Constructs an EtxMqttService with the specified parameters.
   *
   * @param mqttOutboundChannel The channel for outbound MQTT messages
   * @param partnerApiProperties Properties for accessing ETX configuration
   * @param mqttProperties MQTT configuration properties
   * @param registry Metrics registry for monitoring
   */
  public EtxMqttPublishService(MessageChannel mqttOutboundChannel,
      EtxPartnerApiProperties partnerApiProperties, EtxMqttProperties mqttProperties,
      MeterRegistry registry) {
    this.mqttOutboundChannel = mqttOutboundChannel;
    this.partnerApiProperties = partnerApiProperties;
    this.maxMessagesPerSecond = mqttProperties.getMaxMessagesPerSecond();
    this.requireSessionId = mqttProperties.isRequireSessionId();

    this.rateLimitSkippedCounter = Counter.builder("mec-deposit.etx.mqtt.ratelimit.skipped")
        .description("Number of messages skipped due to rate limiting").register(registry);

    // Add gauge metric for current publish rate
    registry.gauge("mec-deposit.etx.mqtt.publish.rate", currentRate);

    this.availableTokens = new AtomicInteger(maxMessagesPerSecond);

    // Modify the scheduler to refill tokens instead of just counting
    scheduler.scheduleAtFixedRate(() -> {
      availableTokens.set(maxMessagesPerSecond);
      int count = messageCount.getAndSet(0);
      currentRate.set(count);
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
   * @param topic The MQTT topic to publish to
   * @param asn1Bytes The ASN.1 encoded message bytes
   * @param retain Whether to retain the message on the broker
   */
  public void publishAsn1Bytes(String topic, byte[] asn1Bytes, boolean retain) {
    // Rate limiting logic - this must be thread safe (before it wasn't)
    if (availableTokens.decrementAndGet() < 0) {
      availableTokens.incrementAndGet();
      rateLimitSkippedCounter.increment();
      log.debug("Skipping message publish - exceeded rate limit of {} Hz", maxMessagesPerSecond);
      return;
    }

    // Check if we have a Verizon ETX session ID before attempting to publish
    if (requireSessionId && !hasEtxSessionId()) {
      log.warn("No Verizon ETX session ID available, skipping message publish to topic: {}", topic);
      log.info("Waiting for Verizon ETX session ID from vzimp/1/ClientInfo topic...");
      availableTokens.incrementAndGet(); // Return the token since we're not using it
      return;
    }


    try {
      Message<byte[]> message =
          MessageBuilder.withPayload(asn1Bytes).setHeader(MqttHeaders.TOPIC, topic)
              .setHeader(MqttHeaders.RETAINED, retain).setHeader(MqttHeaders.QOS, 0).build();

      log.debug("Attempting to publish message to topic: {} (size: {} bytes)", topic,
          asn1Bytes.length);
      boolean sent = mqttOutboundChannel.send(message, 1000);
      if (!sent) {
        throw new RuntimeException("Failed to send message to MQTT channel");
      }

      messageCount.incrementAndGet();
      log.debug("Successfully published message to topic: {}", topic);
    } catch (Exception e) {
      log.error("Error publishing message to topic {}: {}", topic, e.getMessage());
      log.error("Message size: {} bytes, Topic: {}", asn1Bytes.length, topic);
      throw e;
    }
  }

  /**
   * Checks if we have a Verizon ETX session ID.
   *
   * @return true if session ID is available, false otherwise
   */
  private boolean hasEtxSessionId() {
    try {
      String configPath = partnerApiProperties.getCertificatePath() + "/config.json";
      RegistrationConfiguration config = EtxUtil.readConfigFile(configPath);
      boolean hasSessionId =
          config.getEtxSessionID() != null && config.getEtxSessionID().getSessionId() != null
              && !config.getEtxSessionID().getSessionId().trim().isEmpty();

      if (hasSessionId) {
        log.debug("Verizon ETX session ID available: {}", config.getEtxSessionID().getSessionId());
      } else {
        log.debug("No Verizon ETX session ID available yet");
      }
      return hasSessionId;
    } catch (Exception e) {
      log.debug("Error checking Verizon ETX session ID: {}", e.getMessage());
      return false;
    }
  }

}
