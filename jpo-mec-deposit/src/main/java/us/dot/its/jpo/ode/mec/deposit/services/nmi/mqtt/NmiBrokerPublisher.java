package us.dot.its.jpo.ode.mec.deposit.services.nmi.mqtt;

import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.BrokerPublishPayload;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttBrokerTarget;
import us.dot.its.jpo.ode.mec.deposit.services.mqtt.BrokerPublisher;
import us.dot.its.jpo.ode.mec.deposit.services.mqtt.SignedMessageValidationService;

/**
 * NMI target publisher.
 */
@Component
public class NmiBrokerPublisher implements BrokerPublisher {
  private final NmiMqttPublishService nmiMqttPublishService;
  private final SignedMessageValidationService signedMessageValidationService;

  public NmiBrokerPublisher(NmiMqttPublishService nmiMqttPublishService,
      SignedMessageValidationService signedMessageValidationService) {
    this.nmiMqttPublishService = nmiMqttPublishService;
    this.signedMessageValidationService = signedMessageValidationService;
  }

  @Override
  public MqttBrokerTarget target() {
    return MqttBrokerTarget.NMI;
  }

  @Override
  public void publish(BrokerPublishPayload payload, boolean retain) {
    signedMessageValidationService.validateIfSigned(payload.getPayload(), payload.getTopics(), "NMI");
    for (String topic : payload.getTopics()) {
      nmiMqttPublishService.publishAsn1Bytes(topic, payload.getPayload(), retain);
    }
  }
}
