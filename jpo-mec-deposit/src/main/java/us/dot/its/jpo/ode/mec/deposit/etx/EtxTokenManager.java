package us.dot.its.jpo.ode.mec.deposit.etx;

import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.ode.mec.deposit.etx.models.partner.AuthToken;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class EtxTokenManager {
  private String currentToken;
  private long expirationTime;
  private final EtxApi impApi;

  public EtxTokenManager(EtxApi impApi) {
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
      log.info("ETX token refreshed, valid for {} seconds", authToken.getExpiresIn());
    } else {
      log.error("Failed to refresh ETX token");
    }
  }
}
