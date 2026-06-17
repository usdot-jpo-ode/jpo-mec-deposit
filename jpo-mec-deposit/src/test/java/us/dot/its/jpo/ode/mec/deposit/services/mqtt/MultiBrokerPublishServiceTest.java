package us.dot.its.jpo.ode.mec.deposit.services.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.BrokerPublishPayload;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttBrokerTarget;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttFanoutPublishResult;

class MultiBrokerPublishServiceTest {

  @Test
  void dualPublish_shouldIsolateFailuresPerTarget() {
    BrokerPublisher etxPublisher = mock(BrokerPublisher.class);
    BrokerPublisher nmiPublisher = mock(BrokerPublisher.class);
    when(etxPublisher.target()).thenReturn(MqttBrokerTarget.ETX);
    when(nmiPublisher.target()).thenReturn(MqttBrokerTarget.NMI);
    doThrow(new RuntimeException("NMI down")).when(nmiPublisher).publish(any(), anyBoolean());

    MultiBrokerPublishService service = new MultiBrokerPublishService(
        List.of(etxPublisher, nmiPublisher),
        Set.of(MqttBrokerTarget.ETX, MqttBrokerTarget.NMI),
        new SimpleMeterRegistry());

    Map<MqttBrokerTarget, BrokerPublishPayload> payloads = Map.of(
        MqttBrokerTarget.ETX, BrokerPublishPayload.builder().target(MqttBrokerTarget.ETX)
            .topics(Set.of("etx/topic")).payload(new byte[] {1}).build(),
        MqttBrokerTarget.NMI, BrokerPublishPayload.builder().target(MqttBrokerTarget.NMI)
            .topics(Set.of("nmi/topic")).payload(new byte[] {2}).build());

    MqttFanoutPublishResult result = service.publish(payloads, false);
    assertEquals(Set.of("ETX"), result.mqttBrokerTargets());
    assertEquals(Set.of("etx/topic"), result.publishedTopics());
    verify(etxPublisher).publish(any(), anyBoolean());
    verify(nmiPublisher).publish(any(), anyBoolean());
  }

  @Test
  void singleNmiPublisher_targetsNmi() {
    BrokerPublisher nmiPublisher = mock(BrokerPublisher.class);
    when(nmiPublisher.target()).thenReturn(MqttBrokerTarget.NMI);

    MultiBrokerPublishService service = new MultiBrokerPublishService(
        List.of(nmiPublisher),
        Set.of(MqttBrokerTarget.NMI),
        new SimpleMeterRegistry());

    assertTrue(service.isTargetActive(MqttBrokerTarget.NMI));
    Map<MqttBrokerTarget, BrokerPublishPayload> payloads =
        Map.of(MqttBrokerTarget.NMI, BrokerPublishPayload.builder().target(MqttBrokerTarget.NMI)
            .topics(Set.of("nmi/topic")).payload(new byte[] {2}).build());
    MqttFanoutPublishResult result = service.publish(payloads, false);
    assertEquals(Set.of("NMI"), result.mqttBrokerTargets());
    assertEquals(Set.of("nmi/topic"), result.publishedTopics());
    assertEquals(Set.of("nmi/topic"), MultiBrokerPublishService.unionPayloadTopics(payloads));
    verify(nmiPublisher).publish(any(), anyBoolean());
  }

  @Test
  void singleAvPublisher_targetsAv() {
    BrokerPublisher avPublisher = mock(BrokerPublisher.class);
    when(avPublisher.target()).thenReturn(MqttBrokerTarget.AV);

    MultiBrokerPublishService service = new MultiBrokerPublishService(
        List.of(avPublisher),
        Set.of(MqttBrokerTarget.AV),
        new SimpleMeterRegistry());

    assertTrue(service.isTargetActive(MqttBrokerTarget.AV));
    Map<MqttBrokerTarget, BrokerPublishPayload> payloads =
        Map.of(MqttBrokerTarget.AV, BrokerPublishPayload.builder().target(MqttBrokerTarget.AV)
            .topics(Set.of("av/topic")).payload(new byte[] {3}).build());
    MqttFanoutPublishResult result = service.publish(payloads, false);
    assertEquals(Set.of("AV"), result.mqttBrokerTargets());
    assertEquals(Set.of("av/topic"), result.publishedTopics());
    verify(avPublisher).publish(any(), anyBoolean());
  }
}
