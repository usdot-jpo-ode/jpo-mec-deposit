package us.dot.its.jpo.ode.mec.deposit.etx.depositors;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttMessageType;
import us.dot.its.jpo.ode.mec.deposit.utils.DateJsonMapper;

/**
 * Abstract base class for ETX depositors.
 */
@Slf4j
public abstract class AbstractEtxDepositor {
  protected final ObjectMapper mapper;
  protected final Timer processingTimer;
  protected final Counter staleMessageCounter;
  protected final EtxProperties etxProperties;
  protected final EtxMqttMessageType messageType;
  protected final int staleMessageThreshold;

  protected AbstractEtxDepositor(EtxProperties etxProperties, EtxMqttMessageType messageType,
      MeterRegistry registry, String metricsPrefix) {
    this.etxProperties = etxProperties;
    this.messageType = messageType;
    this.staleMessageThreshold = etxProperties.getMqtt().getStaleMessageThreshold();
    this.processingTimer =
        Timer.builder(metricsPrefix + ".processing").tag("message.type", messageType.name())
            .description("Time taken to process " + messageType.name() + " messages")
            .register(registry);
    this.staleMessageCounter =
        Counter.builder(metricsPrefix + ".stale").tag("message.type", messageType.name())
            .description("Number of stale " + messageType.name() + " messages").register(registry);
    this.mapper = DateJsonMapper.getInstance();
  }

  protected void recordLatency(String odeReceivedAt, LocalDateTime startTime) {
    LocalDateTime msgTimestamp =
        LocalDateTime.parse(odeReceivedAt, DateTimeFormatter.ISO_DATE_TIME);
    Duration latency = Duration.between(msgTimestamp, startTime);
    log.debug("{} processing latency: {} milliseconds", getProcessingType(), latency.toMillis());
    processingTimer.record(latency);
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

  protected abstract String getProcessingType();
}
