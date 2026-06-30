package us.dot.its.jpo.ode.mec.deposit.utils;

import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;
import us.dot.its.jpo.ode.uper.UperUtil;

/**
 * Utility class for detecting message types from raw ASN.1/J2735 payloads. Uses start-flag scanning
 * (the same approach as {@link UperUtil}) to identify the message type, which correctly handles
 * payloads that may be prefixed with IEEE 1609.2/1609.3 security or transport headers.
 */
@Slf4j
public class MessageTypeDetector {

  private MessageTypeDetector() {
    throw new UnsupportedOperationException();
  }

  /**
   * Detects the J2735 message type from raw ASN.1 payload bytes by scanning for known start flags
   * via {@link UperUtil#determineHexPacketType(String)}. This approach handles signed/unsigned
   * 1609.2 and 1609.3 headers transparently, unlike a fixed-offset DSRC message ID read.
   *
   * @param messageBytes The raw message bytes (may include 1609.2/1609.3 headers)
   * @return The detected {@link EtxMessageType}, or {@code null} if the type cannot be determined
   */
  public static EtxMessageType detectMessageType(byte[] messageBytes) {
    try {
      String hexString = Hex.toHexString(messageBytes).toLowerCase();
      String detectedType = UperUtil.determineHexPacketType(hexString);
      if (detectedType == null || detectedType.isEmpty()) {
        log.warn("Could not determine message type from hex payload");
        return null;
      }
      return mapToEtxMessageType(detectedType);
    } catch (Exception e) {
      log.warn("Failed to extract message type from payload", e);
      return null;
    }
  }

  /**
   * Maps the string type returned by {@link UperUtil#determineHexPacketType(String)} to the
   * corresponding {@link EtxMessageType}.
   *
   * @param typeString Type string as returned by UperUtil (e.g. "BSM", "TIM")
   * @return The matching {@link EtxMessageType}, or {@code null} for types with no ETX equivalent
   */
  private static EtxMessageType mapToEtxMessageType(String typeString) {
    switch (typeString) {
      case "BSM":
        return EtxMessageType.BSM;
      case "PSM":
        return EtxMessageType.PSM;
      case "TIM":
        return EtxMessageType.TIM;
      case "MAP":
        return EtxMessageType.MAP;
      case "SPAT":
        return EtxMessageType.SPAT;
      case "SDSM":
        return EtxMessageType.SDSM;
      case "RSM":
        return EtxMessageType.RSA;
      default:
        log.warn("No EtxMessageType mapping for detected type: {}", typeString);
        return null;
    }
  }

  /**
   * Maps message type to its DSRC message ID (decimal string), used for NMI-style topic suffixes
   * and alignment with signed-message header metadata.
   *
   * @param messageType Message type to map
   * @return Decimal DSRCmsgID value as a string
   */
  public static String getDsrcMsgIdForMessageType(EtxMessageType messageType) {
    if (messageType == null) {
      return null;
    }
    switch (messageType) {
      case BSM:
        return "20";
      case PSM:
        return "32";
      case SPAT:
        return "19";
      case TIM:
        return "31";
      case SDSM:
        return "41";
      case MAP:
        return "18";
      default:
        return null;
    }
  }
}
