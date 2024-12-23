package us.dot.its.jpo.ode.mec.deposit.imp;

import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.ode.mec.deposit.models.imp.ImpConfigData;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.concurrent.TimeUnit;

/**
 * Service responsible for handling IMP client registration.
 */
@Component
@Slf4j
@ConditionalOnProperty(value = {"depositor.imp.enabled"}, havingValue = "true")
public class ImpApiService {
  private final ImpApi partnerApi;
  private final ImpTokenManager tokenManager;


  /**
   * Creates a new IMP registration service and attempts to register the client.
   *
   * @param properties The IMP configuration properties
   */
  public ImpApiService(ImpApi impApi, ImpTokenManager tokenManager) {
    this.partnerApi = impApi;
    this.tokenManager = tokenManager;
  }

  public void registerClientPartner() {
    String token = tokenManager.getValidToken();
    ImpConfigData response = partnerApi.registerClientPartner(token);

    if (response != null) {
      log.info("IMP registration successful");
    } else {
      log.error("IMP registration failed with response: {}", response);
    }
  }

  @ConditionalOnProperty(value = "depositor.imp.clear-tim.enabled", havingValue = "true")
  @Scheduled(fixedRateString = "${depositor.imp.clear-tim.interval}", timeUnit = TimeUnit.MINUTES)
  public void clearTim() {
    String token = tokenManager.getValidToken();
    partnerApi.clearTim(token);
  }
}
