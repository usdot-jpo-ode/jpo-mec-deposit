package us.dot.its.jpo.ode.mec.deposit.imp.mqtt;

import us.dot.its.jpo.ode.mec.deposit.DepositorProperties;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttMessageFormat;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttMessageType;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.ImpMqttRegionalTopic;
import us.dot.its.jpo.ode.plugin.j2735.OdePosition3D;
import ch.hsr.geohash.GeoHash;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ImpMqttTopicBuilder {
    private static final String MQTT_PREFIX = "vzimp";
    private static final String MQTT_SCHEMA_VERSION = "1";
    private static final String MQTT_TOPIC_TYPE = "Regional";
    private static final String MQTT_PUB_WILDCARD = "-";
    private static final String MQTT_SUB_WILDCARD = "+";

    public static String getRegionalTopic(ImpMqttRegionalTopic topic) {
        return String.format("%s/%s/%s/%s/%s/%s/%s/%s/%s", MQTT_PREFIX, MQTT_SCHEMA_VERSION,
                MQTT_TOPIC_TYPE, topic.getMqttGeohash(), topic.getClientType(),
                topic.getClientSubType(), topic.getVendorId(), topic.getMessageFormat(),
                topic.getMessageType());
    }

    public static String getPubTopicGeoHash(String geoHash, int precision) {
        if (geoHash == null) {
            throw new IllegalArgumentException("geoHash cannot be null");
        }
        if (geoHash.isEmpty()) {
            throw new IllegalArgumentException("geoHash cannot be empty");
        }
        if (precision < 6 || precision > 8) {
            throw new IllegalArgumentException("precision must be between 7 and 8");
        }

        // Truncate if longer than 8 chars
        if (geoHash.length() > 8) {
            geoHash = geoHash.substring(0, 8);
        }
        // Pad with wildcards if shorter than 8
        else if (geoHash.length() < 8) {
            geoHash = geoHash + MQTT_PUB_WILDCARD.repeat(8 - geoHash.length());
        }

        String topicGeohash = String.join("/", geoHash.split(""));
        return topicGeohash;
    }

    public static String getPubGeoHash(double latitude, double longitude, int precision) {
        String geoHash = GeoHash.withCharacterPrecision(latitude, longitude, precision).toBase32();
        return getPubTopicGeoHash(geoHash, precision);
    }

    public static String buildRegionalTopic(OdePosition3D refPoint, int precision,
            DepositorProperties properties, ImpMqttMessageFormat messageFormat,
            ImpMqttMessageType messageType) {
        String geoHash = getPubGeoHash(refPoint.getLatitude().doubleValue(),
                refPoint.getLongitude().doubleValue(), precision);
        ImpMqttRegionalTopic topic = ImpMqttRegionalTopic.builder().mqttGeohash(geoHash)
                .vendorId(properties.getImpMqttVendor()).messageFormat(messageFormat)
                .messageType(messageType).clientType(properties.getImpClientType())
                .clientSubType(properties.getImpClientSubType()).build();
        return getRegionalTopic(topic);
    }
}
