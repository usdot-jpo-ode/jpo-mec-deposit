package us.dot.its.jpo.ode.mec.deposit.etx.depositors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.depositors.AbstractEtxDepositor;
import us.dot.its.jpo.ode.mec.deposit.etx.models.mqtt.EtxMqttMessageType;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;

class AbstractEtxDepositorTest {

  private AbstractEtxDepositor depositor;
  private EtxProperties etxProperties;
  private MeterRegistry registry;
  private Timer timer;
  private Counter counter;

  @BeforeEach
  void setUp() {
    etxProperties = mock(EtxProperties.class);
    EtxMqttProperties mqttProperties = mock(EtxMqttProperties.class);
    when(etxProperties.getMqtt()).thenReturn(mqttProperties);
    when(mqttProperties.getStaleMessageThreshold()).thenReturn(5000);

    registry = mock(MeterRegistry.class);
    timer = mock(Timer.class);
    counter = mock(Counter.class);

    Timer.Builder timerBuilder = mock(Timer.Builder.class);
    when(Timer.builder(anyString())).thenReturn(timerBuilder);
    when(timerBuilder.tag(anyString(), anyString())).thenReturn(timerBuilder);
    when(timerBuilder.description(anyString())).thenReturn(timerBuilder);
    when(timerBuilder.register(any(MeterRegistry.class))).thenReturn(timer);

    Counter.Builder counterBuilder = mock(Counter.Builder.class);
    when(Counter.builder(anyString())).thenReturn(counterBuilder);
    when(counterBuilder.tag(anyString(), anyString())).thenReturn(counterBuilder);
    when(counterBuilder.description(anyString())).thenReturn(counterBuilder);
    when(counterBuilder.register(any(MeterRegistry.class))).thenReturn(counter);

    depositor = new AbstractEtxDepositor(etxProperties, EtxMqttMessageType.TIM, registry, "test") {
      @Override
      protected String getProcessingType() {
        return "TEST";
      }
    };
  }

  @Test
  void testRecordLatency() {
    String odeReceivedAt =
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME);
    LocalDateTime startTime = LocalDateTime.now(ZoneOffset.UTC);

    depositor.recordLatency(odeReceivedAt, startTime);

    verify(timer).record(Duration.ofSeconds(10));
  }

  @Test
  void testIsMessageStale_whenMessageIsStale() {
    String staleTimestamp =
        LocalDateTime.now(ZoneOffset.UTC).minusSeconds(10).format(DateTimeFormatter.ISO_DATE_TIME);

    boolean result = depositor.isMessageStale(staleTimestamp);

    assertTrue(result);
    verify(counter).increment();
  }

  @Test
  void testIsMessageStale_whenMessageIsFresh() {
    String freshTimestamp =
        LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME);

    boolean result = depositor.isMessageStale(freshTimestamp);

    assertFalse(result);
  }
}
