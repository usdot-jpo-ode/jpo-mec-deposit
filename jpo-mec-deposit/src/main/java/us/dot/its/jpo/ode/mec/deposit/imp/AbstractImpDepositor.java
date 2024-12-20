package us.dot.its.jpo.ode.mec.deposit.imp;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.ode.mec.deposit.imp.mqtt.ImpMqttService;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttMessageType;
import us.dot.its.jpo.ode.mec.deposit.utils.DateJsonMapper;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.MeterRegistry;

@Slf4j
public abstract class AbstractImpDepositor {
    protected final ObjectMapper mapper = DateJsonMapper.getInstance();
    protected final ImpMqttService mqttService;
    protected final Timer processingTimer;
    protected final ImpProperties impProperties;
    protected final ImpMqttMessageType messageType;

    protected AbstractImpDepositor(ImpProperties impProperties, ImpMqttService mqttService,
            ImpMqttMessageType messageType, MeterRegistry registry) {
        this.impProperties = impProperties;
        this.mqttService = mqttService;
        this.messageType = messageType;
        this.processingTimer = Timer.builder("imp.message.processing")
                .tag("message.type", messageType.name())
                .description("Time taken to process " + messageType.name() + " messages")
                .register(registry);
    }

    protected void recordLatency(String receivedAt, LocalDateTime startTime) {
        LocalDateTime receivedAtTime = LocalDateTime.parse(receivedAt,
                DateTimeFormatter.ISO_DATE_TIME);
        Duration latency = Duration.between(receivedAtTime, startTime);
        log.debug("Kafka processing latency: {} milliseconds", latency.toMillis());
        processingTimer.record(latency);
    }
}