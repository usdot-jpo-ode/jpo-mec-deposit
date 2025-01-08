package us.dot.its.jpo.ode.mec.deposit.etx;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import lombok.extern.slf4j.Slf4j;
import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.pem.util.PemUtils;
import us.dot.its.jpo.ode.mec.deposit.etx.models.EtxConfigData;
import us.dot.its.jpo.ode.mec.deposit.utils.DateJsonMapper;

/**
 * Utility class for ETX-related operations.
 */
@Slf4j
public class EtxUtil {
  protected static final ObjectMapper mapper = DateJsonMapper.getInstance();

  /**
   * Creates an SSL socket factory from certificate files.
   *
   * @param caCertPath Path to CA certificate
   * @param clientCertPath Path to client certificate
   * @param privateKeyPath Path to private key
   * @return Configured SSL socket factory
   */
  public static SSLSocketFactory createSocketFactory(String caCertPath, String clientCertPath,
      String privateKeyPath) {
    // Convert to absolute paths
    String absoluteCaCertPath = Paths.get(caCertPath).toAbsolutePath().toString();
    String absoluteClientCertPath = Paths.get(clientCertPath).toAbsolutePath().toString();
    String absolutePrivateKeyPath = Paths.get(privateKeyPath).toAbsolutePath().toString();

    log.info("CA Cert Path: {}", absoluteCaCertPath);
    log.info("Client Cert Path: {}", absoluteClientCertPath);
    log.info("Private Key Path: {}", absolutePrivateKeyPath);

    // Check if files exist
    if (!new File(absoluteCaCertPath).exists()) {
      throw new IllegalArgumentException(
          "CA Certificate file not found at path: " + absoluteCaCertPath);
    }
    if (!new File(absoluteClientCertPath).exists()) {
      throw new IllegalArgumentException(
          "Client Certificate file not found at path: " + absoluteClientCertPath);
    }
    if (!new File(absolutePrivateKeyPath).exists()) {
      throw new IllegalArgumentException(
          "Private Key file not found at path: " + absolutePrivateKeyPath);
    }

    X509ExtendedKeyManager keyManager = PemUtils
        .loadIdentityMaterial(Paths.get(absoluteClientCertPath), Paths.get(absolutePrivateKeyPath));
    X509ExtendedTrustManager trustManager =
        PemUtils.loadTrustMaterial(Paths.get(absoluteCaCertPath));

    var sslFactory = SSLFactory.builder().withIdentityMaterial(keyManager)
        .withTrustMaterial(trustManager).build();

    var sslSocketFactory = sslFactory.getSslSocketFactory();
    return sslSocketFactory;
  }

  /**
   * Writes content to a file.
   *
   * @param filePath Path to target file
   * @param content Content to write
   */
  public static void writeToFile(String filePath, String content) {
    try {
      Files.createDirectories(Paths.get(filePath).getParent());
      try (FileWriter writer = new FileWriter(filePath)) {
        writer.write(content);
      }
    } catch (IOException e) {
      log.error("writeToFile IOException: " + e.getStackTrace());
    }
  }

  /**
   * Reads and parses an ETX config file.
   *
   * @param filePath Path to config file
   * @return Parsed config data or null if error occurs
   */
  public static EtxConfigData readConfigFile(String filePath) {
    try {
      String fileContent = new String(Files.readAllBytes(Paths.get(filePath)));
      return mapper.readValue(fileContent, EtxConfigData.class);
    } catch (IOException e) {
      log.error("writeToFile IOException: " + e.getStackTrace());
      return null;
    }
  }

}
