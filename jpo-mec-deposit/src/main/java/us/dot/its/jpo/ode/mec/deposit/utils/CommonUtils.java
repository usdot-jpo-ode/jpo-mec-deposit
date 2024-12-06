package us.dot.its.jpo.ode.mec.deposit.utils;

import java.io.FileWriter;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;
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
}
