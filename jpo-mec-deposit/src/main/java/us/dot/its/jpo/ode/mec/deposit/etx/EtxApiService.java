package us.dot.its.jpo.ode.mec.deposit.etx;

import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.ode.mec.deposit.etx.models.EtxConfigData;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.concurrent.TimeUnit;

/**
 * Service responsible for handling ETX client registration.
 */
@Component
@Slf4j
@ConditionalOnProperty(value = {"etx.enabled"}, havingValue = "true")
public class EtxApiService {
  private final EtxApi partnerApi;
  private final EtxTokenManager tokenManager;


  /**
   * Creates a new ETX registration service and attempts to register the client.
   *
   * @param properties The ETX configuration properties
   */
  public EtxApiService(EtxApi impApi, EtxTokenManager tokenManager) {
    this.partnerApi = impApi;
    this.tokenManager = tokenManager;
  }

  public void registerClientPartner() {
    String token = tokenManager.getValidToken();
    EtxConfigData response = partnerApi.registerClientPartner(token);

    if (response != null) {
      log.info("ETX registration successful");
    } else {
      log.error("ETX registration failed with response: {}", response);
    }
  }

  @ConditionalOnProperty(value = "etx.partner-api.clear-tim.enabled", havingValue = "true")
  @Scheduled(fixedRateString = "${etx.partner-api.clear-tim.interval}", timeUnit = TimeUnit.MINUTES)
  public void clearTim() {
    String token = tokenManager.getValidToken();
    partnerApi.clearTim(token);
  }
}
