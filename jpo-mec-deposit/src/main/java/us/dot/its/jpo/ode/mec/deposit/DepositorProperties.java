package us.dot.its.jpo.ode.mec.deposit;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.UUID;
import java.util.List;
import java.util.Arrays;

import jakarta.annotation.PostConstruct;

import org.apache.commons.lang3.SystemUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
// import org.apache.kafka.streams.StreamsConfig;
// import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
// import org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.Setter;
import us.dot.its.jpo.ode.mec.deposit.models.imp.ImpConfigData;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttClientSubType;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttClientType;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.ImpNetworkType;
import us.dot.its.jpo.ode.mec.deposit.utils.CommonUtils;
import lombok.AccessLevel;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;

@Getter
@Setter
@ConfigurationProperties
public class DepositorProperties implements EnvironmentAware {
	private static final Logger logger = LoggerFactory.getLogger(DepositorProperties.class);

	@Autowired
	@Setter(AccessLevel.NONE)
	private Environment env;

	// Confluent Properties
	private boolean confluentCloudEnabled = false;
	private String confluentKey = null;
	private String confluentSecret = null;

	public Boolean getConfluentCloudStatus() {
		return confluentCloudEnabled;
	}

	/*
	 * General Properties
	 */
	private String version;

	@Value("${spring.kafka.bootstrap-servers}")
	private String kafkaBrokers = null;

	@Value("${spring.kafka.consumer.group-id}")
	private String groupId;

	private static final String DEFAULT_KAFKA_PORT = "9092";

	@Setter(AccessLevel.NONE)
	private String hostId;

	@Setter(AccessLevel.NONE)
	private String kafkaBrokerIP = null;

	@Setter(AccessLevel.NONE)
	private String kafkaTopics = null;

	/*
	 * IMP Properties
	 */
	private Boolean impEnabled;
	private String impVendor;
	private ImpNetworkType impNetworkType;
	private String impPartnerApiBaseUri;
	private String impPartnerUser;
	private String impPartnerPass;
	private ImpMqttClientType impClientType;
	private ImpMqttClientSubType impClientSubType;
	private String impTopicType;
	private Boolean impCacheRegistration;
	private BigDecimal impMecLatitude;
	private BigDecimal impMecLongitude;
	private String impCertPath;
	private String impMqttVendor;

	@Setter(AccessLevel.NONE)
	@Autowired
	BuildProperties buildProperties;

	@PostConstruct
	void initialize() {
		setVersion(buildProperties.getVersion());
		logger.info("groupId: {}", buildProperties.getGroup());
		logger.info("artifactId: {}", buildProperties.getArtifact());
		logger.info("version: {}", version);

		String hostname;
		try {
			hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			// Let's just use a random hostname
			hostname = UUID.randomUUID().toString();
			logger.info("Unknown host error: {}, using random", e);
		}

		hostId = hostname;
		logger.info("Host ID: {}", hostId);
		logger.info("Initializing services on host {}", hostId);

		if (kafkaBrokers == null) {

			String kafkaBroker = CommonUtils.getEnvironmentVariable("KAFKA_BROKER_IP");

			logger.info("ode.kafkaBrokers property not defined. Will try KAFKA_BROKER_IP => {}",
					kafkaBrokers);

			if (kafkaBroker == null) {
				logger.warn(
						"Neither ode.kafkaBrokers ode property nor KAFKA_BROKER_IP environment variable are defined. Defaulting to localhost.");
				kafkaBroker = "localhost";
			}

			kafkaBrokers = kafkaBroker + ":" + DEFAULT_KAFKA_PORT;
		}

		String kafkaType = CommonUtils.getEnvironmentVariable("KAFKA_TYPE");
		if (kafkaType != null) {
			confluentCloudEnabled = kafkaType.equals("CONFLUENT");
			if (confluentCloudEnabled) {

				System.out.println("Enabling Confluent Cloud Integration");

				confluentKey = CommonUtils.getEnvironmentVariable("CONFLUENT_KEY");
				confluentSecret = CommonUtils.getEnvironmentVariable("CONFLUENT_SECRET");
			}
		}

		impEnabled = CommonUtils.getEnvironmentVariable("IMP_ENABLED", "false")
				.equalsIgnoreCase("true");

		if (impEnabled) {
			logger.info("IMP is enabled");
			impVendor = CommonUtils.getEnvironmentVariable("IMP_VENDOR");
			String impNetworkTypeStr = CommonUtils.getEnvironmentVariable("IMP_NETWORK_TYPE");
			impNetworkType = ImpNetworkType.fromValue(impNetworkTypeStr);
			impPartnerApiBaseUri = CommonUtils.getEnvironmentVariable("IMP_PARTNER_API_BASE_URI");
			impPartnerUser = CommonUtils.getEnvironmentVariable("IMP_PARTNER_USER");
			impPartnerPass = CommonUtils.getEnvironmentVariable("IMP_PARTNER_PASS");
			String impMecLatitudeStr = CommonUtils.getEnvironmentVariable("IMP_MEC_LATITUDE");
			impMecLatitude = new BigDecimal(impMecLatitudeStr);
			String impMecLongitudeStr = CommonUtils.getEnvironmentVariable("IMP_MEC_LONGITUDE");
			impMecLongitude = new BigDecimal(impMecLongitudeStr);
			impCertPath = CommonUtils.getEnvironmentVariable("IMP_CERT_PATH");
			impCacheRegistration = CommonUtils
					.getEnvironmentVariable("IMP_CACHE_REGISTRATION", "false")
					.equalsIgnoreCase("true");
			impMqttVendor = CommonUtils.getEnvironmentVariable("IMP_MQTT_VENDOR");
			impClientType = ImpMqttClientType
					.fromValue(CommonUtils.getEnvironmentVariable("IMP_CLIENT_TYPE"));
			impClientSubType = ImpMqttClientSubType
					.fromValue(CommonUtils.getEnvironmentVariable("IMP_CLIENT_SUB_TYPE"));
		}

	}

	public String getProperty(String key) {
		return env.getProperty(key);
	}

	public String getProperty(String key, String defaultValue) {
		return env.getProperty(key, defaultValue);
	}

	public Object getProperty(String key, int i) {
		return env.getProperty(key, Integer.class, i);
	}

	@Value("${spring.kafka.bootstrap-servers}")
	public void setKafkaBrokers(String kafkaBrokers) {
		this.kafkaBrokers = kafkaBrokers;
	}

	@Override
	public void setEnvironment(Environment environment) {
		env = environment;
	}
}
