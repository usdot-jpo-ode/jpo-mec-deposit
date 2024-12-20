package us.dot.its.jpo.ode.mec.deposit.imp;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.ode.mec.deposit.DepositorProperties;
import us.dot.its.jpo.ode.mec.deposit.imp.mqtt.ImpMqttService;
import us.dot.its.jpo.ode.mec.deposit.utils.DateJsonMapper;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import io.micrometer.core.instrument.Timer;

@Slf4j
public abstract class AbstractImpDepositor {
    protected final ObjectMapper mapper = DateJsonMapper.getInstance();
    protected final ImpMqttService mqttService;
    protected final DepositorProperties properties;
    protected final Timer processingTimer;

    protected AbstractImpDepositor(DepositorProperties properties, ImpMqttService mqttService,
            Timer processingTimer) {
        this.properties = properties;
        this.mqttService = mqttService;
        this.processingTimer = processingTimer;
    }

    protected void recordLatency(String receivedAt, LocalDateTime startTime) {
        LocalDateTime receivedAtTime = LocalDateTime.parse(receivedAt,
                DateTimeFormatter.ISO_DATE_TIME);
        Duration latency = Duration.between(receivedAtTime, startTime);
        log.debug("Kafka processing latency: {} milliseconds", latency.toMillis());
        processingTimer.record(latency);
    }
}