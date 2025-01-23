package us.dot.its.jpo.ode.mec.deposit.services.etx;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;

@ExtendWith(MockitoExtension.class)
class EtxMqttPublishServiceTest {

  @Mock
  private MessageChannel mqttOutboundChannel;

  @Mock
  private EtxMqttProperties mqttProperties;

  private MeterRegistry meterRegistry;
  private EtxMqttPublishService service;

  @BeforeEach
  void setUp() {
    meterRegistry = new SimpleMeterRegistry();
    when(mqttProperties.getMaxMessagesPerSecond()).thenReturn(100);
    service = new EtxMqttPublishService(mqttOutboundChannel, mqttProperties, meterRegistry);
  }

  @Test
  void publishAsn1Bytes_SuccessfulPublish() {
    // Arrange
    byte[] testBytes = "test message".getBytes();
    String topic = "test/topic";
    when(mqttOutboundChannel.send(any(Message.class), anyLong())).thenReturn(true);

    // Act
    service.publishAsn1Bytes(topic, testBytes, false);

    // Assert
    ArgumentCaptor<Message<?>> messageCaptor = ArgumentCaptor.forClass(Message.class);
    verify(mqttOutboundChannel).send(messageCaptor.capture(), eq(1000L));

    Message<?> capturedMessage = messageCaptor.getValue();
    assertArrayEquals(testBytes, (byte[]) capturedMessage.getPayload());
    assertEquals(topic, capturedMessage.getHeaders().get("mqtt_topic"));
    assertEquals(false, capturedMessage.getHeaders().get("mqtt_retained"));
    assertEquals(0, capturedMessage.getHeaders().get("mqtt_qos"));
  }

  @Test
  void publishAsn1Bytes_FailedPublish() {
    // Arrange
    byte[] testBytes = "test message".getBytes();
    String topic = "test/topic";
    when(mqttOutboundChannel.send(any(Message.class), anyLong())).thenReturn(false);

    // Act & Assert
    assertThrows(RuntimeException.class, () -> service.publishAsn1Bytes(topic, testBytes, false));
  }

  @Test
  void publishAsn1Bytes_ExceedsRateLimit() {
    // Arrange
    byte[] testBytes = "test message".getBytes();
    String topic = "test/topic";
    when(mqttOutboundChannel.send(any(Message.class), anyLong())).thenReturn(true);

    // Act - Try to publish messages faster than the rate limit
    int attempts = 150; // Try more than the rate limit
    int expectedSuccesses = 100; // Should match maxMessagesPerSecond

    for (int i = 0; i < attempts; i++) {
      service.publishAsn1Bytes(topic, testBytes, false);
    }

    // Assert
    // Verify that only maxMessagesPerSecond messages were actually sent
    verify(mqttOutboundChannel, times(expectedSuccesses)).send(any(Message.class), anyLong());

    // Verify rate limit metric was incremented for the excess messages
    assertEquals(attempts - expectedSuccesses,
        meterRegistry.counter("mec-deposit.etx.mqtt.ratelimit.skipped").count());
  }

  @Test
  void publishAsn1Bytes_ChannelThrowsException() {
    // Arrange
    byte[] testBytes = "test message".getBytes();
    String topic = "test/topic";
    when(mqttOutboundChannel.send(any(Message.class), anyLong()))
        .thenThrow(new RuntimeException("Channel error"));

    // Act & Assert
    assertThrows(RuntimeException.class, () -> service.publishAsn1Bytes(topic, testBytes, false));
  }
}
