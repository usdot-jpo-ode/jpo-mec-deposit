package us.dot.its.jpo.ode.mec.deposit.services.etx;

import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxPartnerClient;
import us.dot.its.jpo.ode.mec.deposit.etx.EtxTokenManager;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.RegistrationConfiguration;

/**
 * Service responsible for handling ETX client registration and Clear TIM operations.
 */
@Component
@Slf4j
@ConditionalOnProperty(value = {"mec-deposit.etx.enabled"}, havingValue = "true")
public class EtxApiService {
  private final EtxPartnerClient partnerApi;
  private final EtxTokenManager tokenManager;


  /**
   * Creates a new ETX registration service and attempts to register the client.
   *
   * @param etxApi The ETX API client
   * @param tokenManager The token manager for authentication
   */
  public EtxApiService(EtxPartnerClient etxApi, EtxTokenManager tokenManager) {
    this.partnerApi = etxApi;
    this.tokenManager = tokenManager;
  }

  /**
   * Registers this client with the ETX partner service.
   */
  public void registerClientPartner() {
    String token = tokenManager.getValidToken();
    RegistrationConfiguration response = partnerApi.registerClientPartner(token);

    if (response != null) {
      log.info("ETX registration successful");
    } else {
      log.error("ETX registration failed with response: {}", response);
    }
  }

  /**
   * Clears the inactive API deployed TIMs that are on the ETX Partner API.
   */
  @ConditionalOnProperty(value = "mec-deposit.etx.partner-api.clear-tim.enabled",
      havingValue = "true")
  @Scheduled(fixedRateString = "${mec-deposit.etx.partner-api.clear-tim.interval}",
      timeUnit = TimeUnit.MINUTES)
  public void clearTim() {
    String token = tokenManager.getValidToken();
    partnerApi.clearTim(token);
  }
}
