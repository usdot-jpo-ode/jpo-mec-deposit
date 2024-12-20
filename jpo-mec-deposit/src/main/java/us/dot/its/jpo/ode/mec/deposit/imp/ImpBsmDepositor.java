package us.dot.its.jpo.ode.mec.deposit.imp;

import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import us.dot.its.jpo.ode.mec.deposit.DepositorProperties;
import us.dot.its.jpo.ode.mec.deposit.imp.mqtt.ImpMqttService;
import us.dot.its.jpo.ode.mec.deposit.imp.mqtt.ImpMqttTopicBuilder;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttMessageType;
import us.dot.its.jpo.ode.model.OdeBsmData;
import us.dot.its.jpo.ode.plugin.j2735.J2735Bsm;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Component
@Slf4j
public class ImpBsmDepositor extends AbstractImpDepositor {

    public ImpBsmDepositor(DepositorProperties properties, ImpMqttService mqttService,
            MeterRegistry registry) {
        super(properties, mqttService,
                Timer.builder("imp.message.processing").tag("message.type", "bsm")
                        .description("Time taken to process BSM messages").register(registry));
    }

    @Async("kafkaListenerExecutor")
    @KafkaListener(topics = "topic.OdeBsmJson", groupId = "${spring.kafka.consumer.group-id}-bsm", concurrency = "${listen.concurrency:1}")
    public void bsmDepositListener(String message) {
        try {
            LocalDateTime startTime = LocalDateTime.now(ZoneOffset.UTC);
            OdeBsmData msg = mapper.readValue(message, OdeBsmData.class);
            byte[] asn1Bytes = Hex.decode(msg.getMetadata().getAsn1());

            J2735Bsm bsm = (J2735Bsm) msg.getPayload().getData();
            String topic = ImpMqttTopicBuilder.buildRegionalTopic(bsm.getCoreData().getPosition(),
                    7, properties, ImpMqttMessageFormat.J2735, ImpMqttMessageType.BSM);

            mqttService.publishAsn1Bytes(topic, asn1Bytes, false).exceptionally(throwable -> {
                log.error("Failed to publish BSM message: {}", throwable.getMessage());
                return null;
            });

            recordLatency(msg.getMetadata().getOdeReceivedAt(), startTime);

        } catch (Exception e) {
            log.error("Error processing BSM message", e);
        }
    }
}