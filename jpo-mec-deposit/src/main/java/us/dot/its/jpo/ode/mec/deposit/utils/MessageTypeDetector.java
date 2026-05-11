package us.dot.its.jpo.ode.mec.deposit.utils;

import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxMessageType;

/**
 * Utility class for detecting message types from ASN.1 payloads by reading DSRC message IDs. Maps
 * DSRC message IDs to corresponding EtxMessageType values.
 */
@Slf4j
public class MessageTypeDetector {

  /**
   * Extracts the message type from the ASN.1 payload by reading the DSRC message ID. Maps the DSRC
   * message ID to the corresponding EtxMessageType.
   *
   * @param messageBytes The ASN.1 message bytes
   * @return The detected EtxMessageType, or null if unable to determine
   */
  public static EtxMessageType detectMessageType(byte[] messageBytes) {
    try {
      if (messageBytes.length < 2) {
        log.warn("Message too short to contain DSRC message ID (need at least 2 bytes)");
        return null;
      }

      // Read the first 2 bytes as the message ID
      int messageId = ((messageBytes[0] & 0xFF) << 8) | (messageBytes[1] & 0xFF);

      // Map DSRC message ID to EtxMessageType based on the DSRCmsgID values
      switch (messageId) {
        case 20: // basicSafetyMessage
          return EtxMessageType.BSM;
        case 18: // mapData
          return EtxMessageType.MAP;
        case 19: // signalPhaseAndTimingMessage
          return EtxMessageType.SPAT;
        case 31: // travelerInformation
          return EtxMessageType.TIM;
        case 41: // sensorDataSharingMessage
          return EtxMessageType.SDSM;
        case 32: // personalSafetyMessage
          return EtxMessageType.RSA;
        case 33: // roadSafetyMessage
          return EtxMessageType.RSA;
        default:
          log.warn("Unknown DSRC message ID: {}", messageId);
          return null;
      }
    } catch (Exception e) {
      log.warn("Failed to extract message type from payload", e);
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
