package us.dot.its.jpo.ode.mec.deposit.services.base;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxPartnerClient;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.EtxTokenManager;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxDepositorType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;

/**
 * Abstract base class for ETX API depositors. Extends AbstractEtxDepositor to provide common API
 * deposit functionality.
 */
public abstract class AbstractEtxApiDepositor extends AbstractEtxDepositor {
  protected final EtxPartnerClient partnerApi;
  protected final EtxTokenManager tokenManager;

  protected AbstractEtxApiDepositor(MecDepositProperties mecDepositProperties,
      EtxProperties etxProperties, EtxPartnerClient etxApi, EtxTokenManager tokenManager,
      MeterRegistry registry, KafkaTemplate<String, String> kafkaTemplate,
      EtxMessageType messageType) {
    super(mecDepositProperties, etxProperties, messageType, registry, "mec-deposit.etx.api",
        kafkaTemplate);
    this.partnerApi = etxApi;
    this.tokenManager = tokenManager;
  }

  @Override
  protected EtxDepositorType getDepositorType() {
    return EtxDepositorType.API;
  }
}
