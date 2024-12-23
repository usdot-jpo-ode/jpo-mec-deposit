package us.dot.its.jpo.ode.mec.deposit.imp;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.AuthToken;

@Slf4j
@Component
@ConditionalOnProperty(value = {"depositor.imp.enabled"}, havingValue = "true")
public class ImpTokenManager {
  private String currentToken;
  private long expirationTime;
  private final ImpApi impApi;

  public ImpTokenManager(ImpApi impApi) {
    this.impApi = impApi;
  }

  public synchronized String getValidToken() {
    if (currentToken == null || System.currentTimeMillis() >= expirationTime) {
      refreshToken();
    }
    return currentToken;
  }

  private void refreshToken() {
    AuthToken authToken = impApi.getToken();
    if (authToken != null) {
      currentToken = authToken.getAccessToken();
      // Set expiration 5 minutes before actual expiry to be safe
      expirationTime = System.currentTimeMillis() + (authToken.getExpiresIn() * 1000L) - 300000L;
      log.info("IMP token refreshed, valid for {} seconds", authToken.getExpiresIn());
    } else {
      log.error("Failed to refresh IMP token");
    }
  }
}
