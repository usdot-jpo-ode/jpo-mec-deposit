package us.dot.its.jpo.ode.mec.deposit.imp;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Service responsible for handling IMP client registration.
 */
@ConditionalOnProperty(value = "depositor.imp.enabled", havingValue = "true")
@Component
@Slf4j
public class ImpRegistrationService {

  /**
   * Creates a new IMP registration service and attempts to register the client.
   *
   * @param properties The IMP configuration properties
   */
  public ImpRegistrationService(ImpProperties properties) {
    var registration = new ImpPartnerApi(properties);
    var response = registration.registerClientPartner();

    if (response != null) {
      log.info("IMP registration successful");
    } else {
      log.error("IMP registration failed, services will not be started");
    }
  }
}
