package us.dot.its.jpo.ode.mec.deposit.imp.depositors.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpApi;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpProperties;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpTokenManager;
import us.dot.its.jpo.ode.mec.deposit.imp.models.mqtt.ImpMqttMessageType;
import us.dot.its.jpo.ode.mec.deposit.utils.DateJsonMapper;

/**
 * Abstract base class for IMP MQTT depositors that handles common functionality for processing and
 * publishing messages to IMP API.
 */
@Slf4j
public abstract class AbstractImpApiDepositor {
  protected final ObjectMapper mapper = DateJsonMapper.getInstance();
  protected final ImpApi partnerApi;
  protected final Timer processingTimer;
  protected final Counter staleMessageCounter;
  protected final ImpProperties impProperties;
  protected final ImpMqttMessageType messageType;
  protected final int staleMessageThreshold;
  protected final ImpTokenManager tokenManager;

  protected AbstractImpApiDepositor(ImpProperties impProperties, ImpMqttMessageType messageType,
      MeterRegistry meterRegistry, ImpApi impApi, ImpTokenManager tokenManager) {
    this.impProperties = impProperties;
    this.messageType = messageType;
    this.partnerApi = impApi;
    this.tokenManager = tokenManager;
    this.staleMessageThreshold = impProperties.getMqtt().getStaleMessageThreshold();
    this.processingTimer =
        Timer.builder("imp.api.processing").tag("message.type", messageType.name())
            .description("Time taken to process " + messageType.name() + " messages")
            .register(meterRegistry);
    this.staleMessageCounter = Counter.builder("imp.api.stale")
        .tag("message.type", messageType.name())
        .description("Number of stale " + messageType.name() + " messages").register(meterRegistry);
  }

  protected void recordLatency(String odeReceivedAt, LocalDateTime startTime) {
    LocalDateTime msgTimestamp =
        LocalDateTime.parse(odeReceivedAt, DateTimeFormatter.ISO_DATE_TIME);
    Duration latency = Duration.between(msgTimestamp, startTime);
    log.debug("API processing latency: {} milliseconds", latency.toMillis());
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
}
