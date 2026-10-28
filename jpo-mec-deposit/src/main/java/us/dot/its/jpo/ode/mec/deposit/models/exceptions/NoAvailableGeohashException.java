package us.dot.its.jpo.ode.mec.deposit.models.exceptions;

/**
 * Exception thrown when no available geohash can be found for a polygon deployment.
 */
public class NoAvailableGeohashException extends RuntimeException {
  public NoAvailableGeohashException(String message) {
    super(message);
  }

  public NoAvailableGeohashException(String message, Throwable cause) {
    super(message, cause);
  }
}
