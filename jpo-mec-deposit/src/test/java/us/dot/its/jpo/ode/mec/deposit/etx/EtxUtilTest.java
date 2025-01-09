package us.dot.its.jpo.ode.mec.deposit.etx;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.net.ssl.SSLSocketFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import us.dot.its.jpo.ode.mec.deposit.etx.models.EtxConfigData;

class EtxUtilTest {

  @TempDir
  Path tempDir;

  private Path caCertPath;
  private Path clientCertPath;
  private Path privateKeyPath;
  private Path configPath;

  @BeforeEach
  void setUp() throws Exception {
    // Create temporary certificate files for testing
    Path caCertPath =
        Path.of(getClass().getClassLoader().getResource("ETX/google-public-ca.pem").toURI());
    caCertPath = caCertPath.toAbsolutePath();

    Path clientCertPath =
        Path.of(getClass().getClassLoader().getResource("ETX/google-public-ca.crt").toURI());
    clientCertPath = clientCertPath.toAbsolutePath();

    Path privateKeyPath =
        Path.of(getClass().getClassLoader().getResource("ETX/randomly-generated.key").toURI());
    privateKeyPath = privateKeyPath.toAbsolutePath();

    Path configPath =
        Path.of(getClass().getClassLoader().getResource("ETX/sample-config.json").toURI());
    configPath = configPath.toAbsolutePath();
  }

  @Test
  void createSocketFactory_ValidCertificates_ReturnsSSLSocketFactory() {
    SSLSocketFactory factory = EtxUtil.createSocketFactory(caCertPath.toString(),
        clientCertPath.toString(), privateKeyPath.toString());

    assertNotNull(factory);
  }

  @Test
  void createSocketFactory_MissingCaCert_ThrowsIllegalArgumentException() {
    String nonExistentPath = tempDir.resolve("nonexistent.crt").toString();

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> EtxUtil.createSocketFactory(nonExistentPath, clientCertPath.toString(),
            privateKeyPath.toString()));

    assertTrue(exception.getMessage().contains("CA Certificate file not found"));
  }

  @Test
  void writeToFile_ValidPath_CreatesFile() throws IOException {
    Path testFile = tempDir.resolve("test.txt");
    String content = "Test content";

    EtxUtil.writeToFile(testFile.toString(), content);

    assertTrue(Files.exists(testFile));
    assertEquals(content, Files.readString(testFile));
  }

  @Test
  void writeToFile_InvalidPath_LogsError() {
    String invalidPath = "/invalid/path/test.txt";
    String content = "Test content";

    // Should not throw exception, but log error instead
    assertDoesNotThrow(() -> EtxUtil.writeToFile(invalidPath, content));
  }

  @Test
  void readConfigFile_ValidConfig_ReturnsConfigData() throws IOException {
    // Create a valid JSON config file
    String configJson = """
        {
            "deviceID": "testClient",
            "etxMqttUri": "ssl://test.broker:8883"
        }
        """;
    Files.writeString(configPath, configJson);

    EtxConfigData config = EtxUtil.readConfigFile(configPath.toString());

    assertNotNull(config);
    assertEquals("testClient", config.getDeviceID());
    assertEquals("ssl://test.broker:8883", config.getEtxMqttUri().toString());
  }

  @Test
  void readConfigFile_InvalidJson_ReturnsNull() throws IOException {
    // Create an invalid JSON config file
    Files.writeString(configPath, "invalid json content");

    EtxConfigData config = EtxUtil.readConfigFile(configPath.toString());

    assertNull(config);
  }

  @Test
  void readConfigFile_FileNotFound_ReturnsNull() {
    String nonExistentPath = tempDir.resolve("nonexistent.json").toString();

    EtxConfigData config = EtxUtil.readConfigFile(nonExistentPath);

    assertNull(config);
  }
}
