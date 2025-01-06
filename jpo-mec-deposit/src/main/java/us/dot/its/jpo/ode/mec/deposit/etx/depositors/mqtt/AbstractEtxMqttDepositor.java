package us.dot.its.jpo.ode.mec.deposit.etx.depositors.mqtt;

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
import us.dot.its.jpo.ode.mec.deposit.etx.models.mqtt.EtxMqttMessageType;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttService;
import us.dot.its.jpo.ode.mec.deposit.utils.DateJsonMapper;

/**
 * Abstract base class for ETX MQTT depositors that handles common functionality for processing and
 * publishing messages to MQTT topics.
 */
@Slf4j
public abstract class AbstractEtxMqttDepositor {
  protected final ObjectMapper mapper = DateJsonMapper.getInstance();
  protected final EtxMqttService mqttService;
  protected final Timer processingTimer;
  protected final Counter staleMessageCounter;
  protected final EtxProperties etxProperties;
  protected final EtxMqttMessageType messageType;
  protected final int staleMessageThreshold;

  protected AbstractEtxMqttDepositor(EtxProperties etxProperties, EtxMqttService mqttService,
      EtxMqttMessageType messageType, MeterRegistry registry) {
    this.etxProperties = etxProperties;
    this.mqttService = mqttService;
    this.messageType = messageType;
    this.staleMessageThreshold = etxProperties.getMqtt().getStaleMessageThreshold();
    this.processingTimer =
        Timer.builder("etx.mqtt.processing").tag("message.type", messageType.name())
            .description("Time taken to process " + messageType.name() + " messages")
            .register(registry);
    this.staleMessageCounter =
        Counter.builder("etx.mqtt.stale").tag("message.type", messageType.name())
            .description("Number of stale " + messageType.name() + " messages").register(registry);
  }

  protected void recordLatency(String odeReceivedAt, LocalDateTime startTime) {
    LocalDateTime msgTimestamp =
        LocalDateTime.parse(odeReceivedAt, DateTimeFormatter.ISO_DATE_TIME);
    Duration latency = Duration.between(msgTimestamp, startTime);
    log.debug("Kafka processing latency: {} milliseconds", latency.toMillis());
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
