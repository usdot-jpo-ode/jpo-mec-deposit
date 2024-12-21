package us.dot.its.jpo.ode.mec.deposit.imp.depositors.api;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpPartnerApi;
import us.dot.its.jpo.ode.mec.deposit.imp.ImpProperties;

/**
 * Depositor class for handling MAP messages via IMP API integration.
 */
@ConditionalOnProperty(value = {"depositor.map.enabled", "depositor.imp.enabled"},
    havingValue = "true")
@Component
@Slf4j
public class ImpMapApiDepositor {
  public ImpMapApiDepositor(ImpProperties properties) {
    ImpPartnerApi partnerApi = new ImpPartnerApi(properties);
    partnerApi.getToken();
  }
}
