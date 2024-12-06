package us.dot.its.jpo.ode.mec.deposit.threads;

import org.apache.commons.lang3.SystemUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.ode.mec.deposit.DepositorProperties;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpDepositorService;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpMqttService;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpRegistration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Controller
@Slf4j
public class ServiceThreadController {
    String kafkaType;

    @Autowired
    public ServiceThreadController(DepositorProperties depositorProperties) {
        log.info("Starting ServiceThreadController");
        // this.depositorProperties = depositorProperties;
        // this.kafkaType = depositorProperties.getKafkaType();

        var sm = new ServiceManager(new ServiceThreadFactory("ServiceManager"));

        if (depositorProperties.getImpEnabled()) {
            startImpServices(depositorProperties);
        }

        log.info("All services started!");

    }

    public void startImpServices(DepositorProperties depositorProperties) {
        // log.info("Starting IMP services");
        // var impRegistrationService = new ImpRegistration(depositorProperties);
        // var response = impRegistrationService.registerClientPartner();

        // if (response != null) {
        // var impDepositorService = new ImpDepositorService(depositorProperties);

        // var sm = new ServiceManager(new ServiceThreadFactory("ImpServiceManager"));
        // // sm.submit(impDepositorService);

        // log.info("IMP services started successfully");
        // } else {
        // log.error("IMP registration failed, services will not be started");
        // }

    }

}
