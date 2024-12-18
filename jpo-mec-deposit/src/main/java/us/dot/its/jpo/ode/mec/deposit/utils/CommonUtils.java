package us.dot.its.jpo.ode.mec.deposit.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;

import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;
import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.pem.util.PemUtils;
import us.dot.its.jpo.ode.mec.deposit.models.imp.ConfigData;

@Slf4j
public class CommonUtils {
	private static ObjectMapper objectMapper = new ObjectMapper();

	public static String getEnvironmentVariable(String variableName) {
		String value = System.getenv(variableName);
		return value;
	}

	public static String getEnvironmentVariable(String variableName, String defaultValue) {
		String value = System.getenv(variableName);
		if (value == null || value.equals("")) {
			System.out.println("Something went wrong retrieving the environment variable " + variableName);
			System.out.println("Using default value: " + defaultValue);
			return defaultValue;
		}
		return value;
	}

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

	public static ConfigData readConfigFile(String filePath) {
		try {
			String fileContent = new String(Files.readAllBytes(Paths.get(filePath)));
			ConfigData configData = objectMapper.readValue(fileContent, ConfigData.class);

			return configData;
		} catch (IOException e) {
			log.error("writeToFile IOException: " + e.getStackTrace());
			return null;
		}
	}

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
			throw new IllegalArgumentException("CA Certificate file not found at path: " + absoluteCaCertPath);
		}
		if (!new File(absoluteClientCertPath).exists()) {
			throw new IllegalArgumentException("Client Certificate file not found at path: " + absoluteClientCertPath);
		}
		if (!new File(absolutePrivateKeyPath).exists()) {
			throw new IllegalArgumentException("Private Key file not found at path: " + absolutePrivateKeyPath);
		}

		X509ExtendedKeyManager keyManager = PemUtils.loadIdentityMaterial(Paths.get(absoluteClientCertPath),
				Paths.get(absolutePrivateKeyPath));
		X509ExtendedTrustManager trustManager = PemUtils.loadTrustMaterial(Paths.get(absoluteCaCertPath));

		var sslFactory = SSLFactory.builder().withIdentityMaterial(keyManager).withTrustMaterial(trustManager).build();

		var sslSocketFactory = sslFactory.getSslSocketFactory();
		return sslSocketFactory;
	}
}
