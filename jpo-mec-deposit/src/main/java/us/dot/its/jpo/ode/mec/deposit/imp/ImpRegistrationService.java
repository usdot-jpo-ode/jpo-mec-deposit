package us.dot.its.jpo.ode.mec.deposit.imp;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ImpRegistrationService {

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
