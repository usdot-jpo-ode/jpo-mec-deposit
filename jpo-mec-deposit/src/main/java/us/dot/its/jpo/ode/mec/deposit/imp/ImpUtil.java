package us.dot.its.jpo.ode.mec.deposit.imp;

import com.google.protobuf.ByteString;

import us.dot.its.jpo.ode.mec.deposit.DepositorProperties;
import us.dot.its.jpo.ode.mec.deposit.models.imp.mqtt.MessageFormat;
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