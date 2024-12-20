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
import us.dot.its.jpo.ode.mec.deposit.models.imp.ImpConfigData;

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
			System.out.println(
					"Something went wrong retrieving the environment variable " + variableName);
			System.out.println("Using default value: " + defaultValue);
			return defaultValue;
		}
		return value;
	}

}
