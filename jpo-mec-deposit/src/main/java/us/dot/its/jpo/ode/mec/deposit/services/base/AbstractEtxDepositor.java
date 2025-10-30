package us.dot.its.jpo.ode.mec.deposit.services.base;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxDepositMetrics;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxDepositorType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.DistributionType;
import us.dot.its.jpo.ode.mec.deposit.utils.DateJsonMapper;

/**
 * Abstract base class for ETX depositors.
 */
@Slf4j
public abstract class AbstractEtxDepositor {
  protected final ObjectMapper mapper;
  protected final Timer processingTimer;
  protected final Counter staleMessageCounter;
  protected final Counter errorCounter;
  protected final MecDepositProperties mecDepositProperties;
  protected final EtxProperties etxProperties;
  protected final EtxMessageType messageType;
  protected final int staleMessageThreshold;
  protected final KafkaTemplate<String, String> kafkaTemplate;

  protected AbstractEtxDepositor(MecDepositProperties mecDepositProperties,
      EtxProperties etxProperties, EtxMessageType messageType, MeterRegistry registry,
      String metricsPrefix, KafkaTemplate<String, String> kafkaTemplate) {
    this.mecDepositProperties = mecDepositProperties;
    this.etxProperties = etxProperties;
    this.messageType = messageType;
    this.staleMessageThreshold = etxProperties.getDepositors().getStaleMessageThreshold();
    this.processingTimer =
        Timer.builder(metricsPrefix + ".processing").tag("message.type", messageType.name())
            .description("Time taken to process " + messageType.name() + " messages")
            .register(registry);
    this.staleMessageCounter =
        Counter.builder(metricsPrefix + ".stale").tag("message.type", messageType.name())
            .description("Number of stale " + messageType.name() + " messages").register(registry);
    this.errorCounter =
        Counter.builder(metricsPrefix + ".error").tag("message.type", messageType.name())
            .description("Number of " + messageType.name() + " messages with errors")
            .register(registry);
    this.mapper = DateJsonMapper.getInstance();
    this.kafkaTemplate = kafkaTemplate;
  }

  protected Duration recordLatency(String odeReceivedAt, LocalDateTime startTime) {
    LocalDateTime msgTimestamp =
        LocalDateTime.parse(odeReceivedAt, DateTimeFormatter.ISO_DATE_TIME);
    Duration latency = Duration.between(msgTimestamp, startTime);
    log.debug("{} processing latency: {} milliseconds", getDepositorType(), latency.toMillis());
    processingTimer.record(latency);
    return latency;
  }

  protected boolean isMessageStale(String odeReceivedAt) {
    LocalDateTime msgTimestamp =
        LocalDateTime.parse(odeReceivedAt, DateTimeFormatter.ISO_DATE_TIME);
    LocalDateTime currentTime = LocalDateTime.now(ZoneOffset.UTC);
    Duration latency = Duration.between(msgTimestamp, currentTime);
    boolean isStale = latency.toMillis() > staleMessageThreshold;
    if (isStale) {
      log.debug("Skipping stale {} message", messageType.name());
      staleMessageCounter.increment();
    }
    return isStale;
  }

  /**
   * Overloaded method to check if a message is stale based on seconds and nanos. This method is
   * designed for use with geohash publishers that work with protobuf timestamps.
   *
   * @param seconds The seconds component of the timestamp
   * @param nanos The nanoseconds component of the timestamp
   * @return true if the message is stale, false otherwise
   */
  protected boolean isMessageStale(long seconds, int nanos) {
    Instant msgInstant = Instant.ofEpochSecond(seconds, nanos);
    LocalDateTime msgTimestamp = LocalDateTime.ofInstant(msgInstant, ZoneOffset.UTC);
    LocalDateTime currentTime = LocalDateTime.now(ZoneOffset.UTC);
    Duration latency = Duration.between(msgTimestamp, currentTime);
    boolean isStale = latency.toMillis() > staleMessageThreshold;
    if (isStale) {
      log.debug("Skipping stale {} message (seconds: {}, nanos: {})", messageType.name(), seconds,
          nanos);
      staleMessageCounter.increment();
    }
    return isStale;
  }

  protected void publishMetrics(EtxDepositMetrics metrics) {
    try {
      kafkaTemplate.send(mecDepositProperties.getMetrics().getKafkaTopic(),
          mapper.writeValueAsString(metrics));
    } catch (JsonProcessingException e) {
      log.error("Error publishing metrics", e);
    }
  }

  protected abstract EtxDepositorType getDepositorType();

  protected void handleProcessingError(Exception e, Set<String> topics,
      DistributionType distributionType, long odeReceivedAtMillis, String asn1Hex) {

    if (!mecDepositProperties.getMetrics().isEnabled()) {
      log.debug("Metrics are disabled, skipping error processing");
      return;
    }

    String errorMessage = e.getMessage();
    log.error("Error processing {} message", messageType, e);

    long nowMillis = Instant.now().toEpochMilli();

    // Publish failure metrics with the attempted topic if available
    publishMetrics(EtxDepositMetrics.builder().depositorType(getDepositorType())
        .messageType(messageType).odeReceivedAt(odeReceivedAtMillis).mecDepositedAt(nowMillis)
        .success(false).errorMessage(errorMessage).distributionType(distributionType)
        .topics(topics != null ? topics : null).asn1Hex(asn1Hex).build());
    errorCounter.increment();
  }

  protected void handleProcessingSuccess(Set<String> topics, DistributionType distributionType,
      String odeReceivedAt, LocalDateTime depositedAt, String asn1Hex) {

    if (!mecDepositProperties.getMetrics().isEnabled()) {
      log.debug("Metrics are disabled, skipping success processing");
      return;
    }

    // Convert ISO timestamp string to epoch millis
    long odeReceivedAtMillis = Instant.parse(odeReceivedAt).toEpochMilli();
    long depositedAtMillis = depositedAt.toInstant(ZoneOffset.UTC).toEpochMilli();

    // Publish success metrics
    publishMetrics(EtxDepositMetrics.builder().depositorType(getDepositorType())
        .messageType(messageType).odeReceivedAt(odeReceivedAtMillis)
        .mecDepositedAt(depositedAtMillis).latencyMs(depositedAtMillis - odeReceivedAtMillis)
        .success(true).distributionType(distributionType).topics(topics).asn1Hex(asn1Hex).build());
    recordLatency(odeReceivedAt, depositedAt);
  }
}

