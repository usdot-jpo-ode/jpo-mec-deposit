package us.dot.its.jpo.ode.mec.deposit.imp;

import com.fasterxml.jackson.databind.ObjectMapper;
import us.dot.its.jpo.ode.mec.deposit.models.imp.ImpConfigData;
import us.dot.its.jpo.ode.mec.deposit.utils.DateJsonMapper;
import us.dot.its.jpo.ode.mec.deposit.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

import lombok.extern.slf4j.Slf4j;
import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.pem.util.PemUtils;

@Slf4j
public class ImpUtil {
    protected final static ObjectMapper mapper = DateJsonMapper.getInstance();

    public static List<double[]> generateGeofence(List<double[]> coordinates,
            double bufferDistance) {
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
            geofencePoints.add(
                    new double[] { p1[0] + perpX * bufferDegrees, p1[1] + perpY * bufferDegrees });

            if (i == 0) {
                // Add starting point's other side for complete polygon
                geofencePoints.add(0, new double[] { p1[0] - perpX * bufferDegrees,
                        p1[1] - perpY * bufferDegrees });
            }

            if (i == coordinates.size() - 2) {
                // Add final point's buffer on both sides
                geofencePoints.add(new double[] { p2[0] + perpX * bufferDegrees,
                        p2[1] + perpY * bufferDegrees });
                geofencePoints.add(new double[] { p2[0] - perpX * bufferDegrees,
                        p2[1] - perpY * bufferDegrees });
            }
        }

        // Add points from original line in reverse to complete the polygon
        for (int i = coordinates.size() - 1; i >= 0; i--) {
            double[] p = coordinates.get(i);
            geofencePoints.add(new double[] { p[0], p[1] });
        }

        return geofencePoints;
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

        X509ExtendedKeyManager keyManager = PemUtils.loadIdentityMaterial(
                Paths.get(absoluteClientCertPath), Paths.get(absolutePrivateKeyPath));
        X509ExtendedTrustManager trustManager = PemUtils
                .loadTrustMaterial(Paths.get(absoluteCaCertPath));

        var sslFactory = SSLFactory.builder().withIdentityMaterial(keyManager)
                .withTrustMaterial(trustManager).build();

        var sslSocketFactory = sslFactory.getSslSocketFactory();
        return sslSocketFactory;
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

    public static ImpConfigData readConfigFile(String filePath) {
        try {
            String fileContent = new String(Files.readAllBytes(Paths.get(filePath)));
            ImpConfigData configData = mapper.readValue(fileContent, ImpConfigData.class);

            return configData;
        } catch (IOException e) {
            log.error("writeToFile IOException: " + e.getStackTrace());
            return null;
        }
    }

}