package us.dot.its.jpo.ode.mec.deposit.etx.partner;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.mec.deposit.models.etx.partner.AuthToken;

/**
 * Manages authentication tokens for Partner API interactions. Handles token storage, validation,
 * and refresh.
 */
@Slf4j
@Component
public class PartnerTokenManager {
  private final PartnerClient partnerApiClient;
  private String currentToken;
  private long expirationTime;

  public PartnerTokenManager(PartnerClient etxApi) {
    this.partnerApiClient = etxApi;
  }

  /**
   * Gets a valid authentication token, refreshing if necessary.
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
    AuthToken authToken = partnerApiClient.getToken();
    if (authToken != null) {
      currentToken = authToken.getAccessToken();
      // Set expiration 10 seconds before actual expiry to allow for processing time
      expirationTime = System.currentTimeMillis() + (authToken.getExpiresIn() * 1000L) - 10000L;
      log.info("Partner token refreshed, valid for {} seconds", authToken.getExpiresIn());
    } else {
      log.error("Failed to refresh partner token");
    }
  }
}
