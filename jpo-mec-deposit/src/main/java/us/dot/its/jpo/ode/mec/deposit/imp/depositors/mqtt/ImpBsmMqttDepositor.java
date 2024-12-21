package us.dot.its.jpo.ode.mec.deposit.imp.depositors.mqtt;

import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import us.dot.its.jpo.ode.mec.deposit.imp.ImpProperties;
import us.dot.its.jpo.ode.mec.deposit.imp.mqtt.ImpMqttService;
import us.dot.its.jpo.ode.mec.deposit.imp.mqtt.ImpMqttTopicBuilder;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttMessageType;
import us.dot.its.jpo.ode.model.OdeBsmData;
import us.dot.its.jpo.ode.plugin.j2735.J2735Bsm;

import io.micrometer.core.instrument.MeterRegistry;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Component
@Slf4j
public class ImpBsmMqttDepositor extends AbstractImpMqttDepositor {

    public ImpBsmMqttDepositor(ImpProperties impProperties, ImpMqttService mqttService,
            MeterRegistry registry) {
        super(impProperties, mqttService, ImpMqttMessageType.BSM, registry);
    }

    @ConditionalOnProperty(value = { "depositor.bsm.enabled",
            "depositor.imp.enabled" }, havingValue = "true")
    @Async("kafkaListenerExecutor")
    @KafkaListener(topics = "${depositor.spat.source-bsm-topic}", groupId = "${spring.kafka.consumer.group-id}-bsm", concurrency = "${listen.concurrency:1}")
    public void bsmDepositListener(String message) {
        try {
            LocalDateTime startTime = LocalDateTime.now(ZoneOffset.UTC);
            OdeBsmData msg = mapper.readValue(message, OdeBsmData.class);
            String odeReceivedAt = msg.getMetadata().getOdeReceivedAt();

            if (isMessageStale(odeReceivedAt)) {
                return;
            }

            byte[] asn1Bytes = Hex.decode(msg.getMetadata().getAsn1());

            J2735Bsm bsm = (J2735Bsm) msg.getPayload().getData();
            String topic = ImpMqttTopicBuilder.buildRegionalTopic(messageType,
                    bsm.getCoreData().getPosition(), 7, impProperties);

            mqttService.publishAsn1Bytes(topic, asn1Bytes, false).exceptionally(throwable -> {
                log.error("Failed to publish BSM message: {}", throwable.getMessage());
                return null;
            });

            recordLatency(odeReceivedAt, startTime);

        } catch (Exception e) {
            log.error("Error processing BSM message", e);
        }
    }
}