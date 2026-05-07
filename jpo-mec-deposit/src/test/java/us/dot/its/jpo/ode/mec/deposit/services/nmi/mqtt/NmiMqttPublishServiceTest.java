package us.dot.its.jpo.ode.mec.deposit.services.nmi.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class NmiMqttPublishServiceTest {

  @Test
  void toPahoConnectionUri_mqttToTcp() {
    assertEquals("tcp://mqtt.development.v2x.isscms.com:1883",
        NmiMqttPublishService.toPahoConnectionUri("mqtt://mqtt.development.v2x.isscms.com:1883"));
  }

  @Test
  void toPahoConnectionUri_mqttsToSsl() {
    assertEquals("ssl://broker.example.com:8883",
        NmiMqttPublishService.toPahoConnectionUri("mqtts://broker.example.com:8883"));
  }

  @Test
  void toPahoConnectionUri_tcpUnchanged() {
    assertEquals("tcp://host:1883", NmiMqttPublishService.toPahoConnectionUri("tcp://host:1883"));
  }

  @Test
  void toPahoConnectionUri_nullOrBlank() {
    assertNull(NmiMqttPublishService.toPahoConnectionUri(null));
    assertEquals("", NmiMqttPublishService.toPahoConnectionUri(""));
    assertEquals("", NmiMqttPublishService.toPahoConnectionUri("  "));
  }
}
