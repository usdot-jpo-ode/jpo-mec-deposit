package us.dot.its.jpo.ode.mec.deposit.services.base;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import us.dot.its.jpo.ode.mec.deposit.MecDepositProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.PartnerClient;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.PartnerTokenManager;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxDepositorType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;

/**
 * Abstract base class for API depositors. Extends {@link AbstractDepositor} to provide common API
 * deposit functionality.
 */
public abstract class AbstractApiDepositor extends AbstractDepositor {
  protected final PartnerClient partnerApi;
  protected final PartnerTokenManager tokenManager;

  protected AbstractApiDepositor(MecDepositProperties mecDepositProperties,
      EtxProperties etxProperties, PartnerClient etxApiClient, PartnerTokenManager tokenManager,
      MeterRegistry registry, KafkaTemplate<String, String> kafkaTemplate,
      EtxMessageType messageType) {
    super(mecDepositProperties, etxProperties, messageType, registry, "mec-deposit.etx.api",
        kafkaTemplate);
    this.partnerApi = etxApiClient;
    this.tokenManager = tokenManager;
  }

  @Override
  protected EtxDepositorType getDepositorType() {
    return EtxDepositorType.API;
  }
}
