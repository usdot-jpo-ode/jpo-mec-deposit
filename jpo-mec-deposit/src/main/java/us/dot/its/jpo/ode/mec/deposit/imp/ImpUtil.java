package us.dot.its.jpo.ode.mec.deposit.imp;

import com.google.protobuf.ByteString;

import us.dot.its.jpo.ode.mec.deposit.GeoRoutedMsg;
import us.dot.its.jpo.ode.mec.deposit.DepositorProperties;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.MessageFormat;
import us.dot.its.jpo.ode.mec.deposit.models.ode.*;
import us.dot.its.jpo.ode.model.OdeTimData;
import us.dot.its.jpo.ode.plugin.j2735.timstorage.Anchor;
import us.dot.its.jpo.ode.mec.deposit.*;

import com.google.protobuf.Timestamp;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import ch.hsr.geohash.GeoHash;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ImpUtil {
    public static GeoRoutedMsg getGeoRoutedMsg(String asn1String, String odeReceivedAt, double latitude,
            double longitude) {
        ByteString asn1ByteString = ByteString.copyFrom(asn1String.getBytes());

        // Parse the odeReceivedAt string to an Instant
        Instant instant = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(odeReceivedAt));
        // Convert the Instant to a Timestamp
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano())
                .build();

        us.dot.its.jpo.ode.mec.deposit.Position position = us.dot.its.jpo.ode.mec.deposit.Position.newBuilder()
                .setLatitude(latitude).setLongitude(longitude).build();

        GeoRoutedMsg geoRoutedMsg = GeoRoutedMsg.newBuilder().setMsgBytes(asn1ByteString).setTime(timestamp)
                .setPosition(position).build();

        return geoRoutedMsg;
    }

    public static String getTopicGeohash(double longitude, double latitude, int precision, String wildcardChar) {
        String geoHash = GeoHash.withCharacterPrecision(latitude, longitude, precision).toBase32();
        if (geoHash.length() < 8) {
            geoHash = geoHash + wildcardChar.repeat(8 - geoHash.length());
        }
        String topicGeohash = String.join("/", geoHash.split(""));
        return topicGeohash;
    }

    public static Set<String> getTopicGeohashList(List<double[]> coords, int precision, String wildcardChar) {
        Set<String> geohashes = new HashSet<>();
        for (double[] coord : coords) {
            String geohash = getTopicGeohash(coord[0], coord[1], precision, wildcardChar);
            geohashes.add(geohash);
        }
        log.info("Geohashes: {}", geohashes);
        return geohashes;
    }

    // Haversine formula to calculate distance between two points in meters
    public static double haversine(double lat1, double lon1, double lat2, double lon2) {
        final int R = 6371000; // Radius of Earth in meters
        double phi1 = Math.toRadians(lat1);
        double phi2 = Math.toRadians(lat2);
        double deltaPhi = Math.toRadians(lat2 - lat1);
        double deltaLambda = Math.toRadians(lon2 - lon1);

        double a = Math.sin(deltaPhi / 2) * Math.sin(deltaPhi / 2)
                + Math.cos(phi1) * Math.cos(phi2) * Math.sin(deltaLambda / 2) * Math.sin(deltaLambda / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return R * c; // Distance in meters
    }

    public static List<double[]> getTimPathCoordList(OdeTimData timMessage) {
        final double thresholdDistance = 10.0; // Threshold distance in meters
        final double odeScaleFactor = 10000000.0;

        // Extract metadata and anchor point
        Anchor msgAnchor = timMessage.getPayload().getData().getMessageFrame().getValue().getTravelerInformation()
                .getDataFrames().getTravelerDataFrame().getRegions().getGeographicalPath().getAnchor();

        // Anchor coordinates in GeoJSON format
        double anchorLat = Double.parseDouble(msgAnchor.getLat()) / odeScaleFactor;
        double anchorLong = Double.parseDouble(msgAnchor.getLong()) / odeScaleFactor;

        // Generate path coordinates from anchor and nodes
        List<double[]> pathCoordinates = new ArrayList<>();
        pathCoordinates.add(new double[] { anchorLong, anchorLat }); // Start with anchor point

        // var region =
        // timMessage.getPayload().getData().getMessageFrame().getValue().getTravelerInformation()
        // .getDataFrames().getTravelerDataFrame().getRegions().getGeographicalPath();

        List<NodeLL> msgNodes = timMessage.getPayload().getData().getMessageFrame().getValue().getTravelerInformation()
                .getDataFrames().getTravelerDataFrame().getRegions().getGeographicalPath().getDescription().getPath()
                .getOffset().getLl().getNodes().getNodeLL();

        double currentLat = anchorLat;
        double currentLong = anchorLong;

        for (NodeLL node : msgNodes) {
            Delta delta = node.getDelta();
            double deltaLong = 0;
            double deltaLat = 0;

            if (delta.getNodeLL1() != null) {
                deltaLong = Double.parseDouble(delta.getNodeLL1().getLon()) / odeScaleFactor;
                deltaLat = Double.parseDouble(delta.getNodeLL1().getLat()) / odeScaleFactor;
            } else if (delta.getNodeLL2() != null) {
                deltaLong = Double.parseDouble(delta.getNodeLL2().getLon()) / odeScaleFactor;
                deltaLat = Double.parseDouble(delta.getNodeLL2().getLat()) / odeScaleFactor;
            } else if (delta.getNodeLL3() != null) {
                deltaLong = Double.parseDouble(delta.getNodeLL3().getLon()) / odeScaleFactor;
                deltaLat = Double.parseDouble(delta.getNodeLL3().getLat()) / odeScaleFactor;
            } else if (delta.getNodeLL4() != null) {
                deltaLong = Double.parseDouble(delta.getNodeLL4().getLon()) / odeScaleFactor;
                deltaLat = Double.parseDouble(delta.getNodeLL4().getLat()) / odeScaleFactor;
            } else if (delta.getNodeLL5() != null) {
                deltaLong = Double.parseDouble(delta.getNodeLL5().getLon()) / odeScaleFactor;
                deltaLat = Double.parseDouble(delta.getNodeLL5().getLat()) / odeScaleFactor;
            } else if (delta.getNodeLL6() != null) {
                deltaLong = Double.parseDouble(delta.getNodeLL6().getLon()) / odeScaleFactor;
                deltaLat = Double.parseDouble(delta.getNodeLL6().getLat()) / odeScaleFactor;
            } else {
                log.error("Error: Delta node not found");
                return null;
            }

            double newLong = currentLong + deltaLong;
            double newLat = currentLat + deltaLat;

            // Calculate the distance from the current point to the new point
            double distance = haversine(currentLat, currentLong, newLat, newLong);
            log.debug("Distance: {}", distance);

            // If the distance is greater than the threshold, add intermediate points
            if (distance > thresholdDistance) {
                int numPoints = (int) Math.ceil(distance / thresholdDistance);
                double deltaLatStep = deltaLat / numPoints;
                double deltaLongStep = deltaLong / numPoints;

                for (int i = 1; i < numPoints; i++) {
                    double intermediateLat = currentLat + i * deltaLatStep;
                    double intermediateLong = currentLong + i * deltaLongStep;
                    pathCoordinates.add(new double[] { intermediateLong, intermediateLat });
                }
            }

            currentLong = newLong;
            currentLat = newLat;
            pathCoordinates.add(new double[] { currentLong, currentLat });
        }
        return pathCoordinates;
    }

    // public static String getTopic()

    public static List<String> getRegionalTimTopicList(OdeTimData timMsg, DepositorProperties properties) {
        List<String> topicList = new ArrayList<>();
        List<double[]> timPathCoordList = ImpUtil.getTimPathCoordList(timMsg);
        Set<String> geohashes = ImpUtil.getTopicGeohashList(timPathCoordList, 8, "-");

        for (String geohash : geohashes) {
            String pubTopic = String.format("vzimp/1/RegionalStatic/%s/%s/%s/%s/j2735_gr/%s", geohash,
                    properties.getImpClientType(), properties.getImpClientSubType(), "Public", "TIM");
            topicList.add(pubTopic);
        }

        return topicList;

    }

    public static List<String> getGenericTopic(BigDecimal lat, BigDecimal lon, Integer precision, String msgType,
            DepositorProperties properties, MessageFormat messageFormat) {
        List<String> topicList = new ArrayList<>();
        // truncating the BigDecimal to double as it doesn't matter for the geohash
        Set<String> geohashes = ImpUtil
                .getTopicGeohashList(List.of(new double[] { lon.doubleValue(), lat.doubleValue() }), precision, "-");

        for (String geohash : geohashes) {
            String pubTopic = String.format("vzimp/1/Regional/%s/%s/%s/%s/%s/%s", geohash,
                    properties.getImpClientType(), properties.getImpClientSubType(), "Public", messageFormat.name(),
                    msgType.toUpperCase());
            topicList.add(pubTopic);
        }

        return topicList;

    }

    public static List<double[]> generateGeofence(List<double[]> coordinates, double bufferDistance) {
        List<double[]> geofencePoints = new ArrayList<>();

        // Need at least 2 points to create a buffer
        if (coordinates.size() < 2) {
            return coordinates;
        }

        // For each line segment, generate perpendicular points at buffer distance
        for (int i = 0; i < coordinates.size() - 1; i++) {
            double[] p1 = coordinates.get(i);
            double[] p2 = coordinates.get(i + 1);

            // Calculate vector of line segment
            double dx = p2[0] - p1[0];
            double dy = p2[1] - p1[1];

            // Calculate perpendicular vector (normalized)
            double length = Math.sqrt(dx * dx + dy * dy);
            double perpX = -dy / length;
            double perpY = dx / length;

            // Convert buffer distance from meters to degrees (approximate)
            double bufferDegrees = bufferDistance / 111320.0; // 1 degree ≈ 111.32 km at equator

            // Add offset points on both sides
            geofencePoints.add(new double[] { p1[0] + perpX * bufferDegrees, p1[1] + perpY * bufferDegrees });

            if (i == 0) {
                // Add starting point's other side for complete polygon
                geofencePoints.add(0, new double[] { p1[0] - perpX * bufferDegrees, p1[1] - perpY * bufferDegrees });
            }

            if (i == coordinates.size() - 2) {
                // Add final point's buffer on both sides
                geofencePoints.add(new double[] { p2[0] + perpX * bufferDegrees, p2[1] + perpY * bufferDegrees });
                geofencePoints.add(new double[] { p2[0] - perpX * bufferDegrees, p2[1] - perpY * bufferDegrees });
            }
        }

        // Add points from original line in reverse to complete the polygon
        for (int i = coordinates.size() - 1; i >= 0; i--) {
            double[] p = coordinates.get(i);
            geofencePoints.add(new double[] { p[0], p[1] });
        }

        return geofencePoints;
    }

}