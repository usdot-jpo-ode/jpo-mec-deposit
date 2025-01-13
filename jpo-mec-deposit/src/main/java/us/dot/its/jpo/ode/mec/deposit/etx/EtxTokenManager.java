package us.dot.its.jpo.ode.mec.deposit.etx;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.AuthToken;

/**
 * Manages authentication tokens for ETX API interactions. Handles token storage, validation, and
 * refresh.
 */
@Slf4j
@Component
public class EtxTokenManager {
  private String currentToken;
  private long expirationTime;
  private final EtxPartnerClient etxApi;

  public EtxTokenManager(EtxPartnerClient etxApi) {
    this.etxApi = etxApi;
  }

  /**
   * Gets a valid authentication token, refreshing if necessary.
   * 
   *
   * @return Valid authentication token
   */
  public synchronized String getValidToken() {
    if (currentToken == null || System.currentTimeMillis() >= expirationTime) {
      refreshToken();
    }
    return currentToken;
  }

  private void refreshToken() {
    AuthToken authToken = etxApi.getToken();
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
