package us.dot.its.jpo.ode.mec.deposit.etx.depositors.api;

import io.micrometer.core.instrument.MeterRegistry;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxApi;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxTokenManager;
import us.dot.its.jpo.ode.mec.deposit.etx.depositors.AbstractEtxDepositor;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.EtxMqttMessageType;

/**
 * Abstract base class for ETX API depositors. Extends AbstractEtxDepositor to provide common API
 * deposit functionality.
 */
public abstract class AbstractImpApiDepositor extends AbstractEtxDepositor {
  protected final EtxApi partnerApi;
  protected final EtxTokenManager tokenManager;

  protected AbstractImpApiDepositor(EtxProperties etxProperties, EtxMqttMessageType messageType,
      MeterRegistry meterRegistry, EtxApi impApi, EtxTokenManager tokenManager) {
    super(etxProperties, messageType, meterRegistry, "etx.api");
    this.partnerApi = impApi;
    this.tokenManager = tokenManager;
  }

  @Override
  protected String getProcessingType() {
    return "API";
  }
}
