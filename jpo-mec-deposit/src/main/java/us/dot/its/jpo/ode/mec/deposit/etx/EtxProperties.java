package us.dot.its.jpo.ode.mec.deposit.etx;

import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.context.annotation.Configuration;
import us.dot.its.jpo.ode.mec.deposit.etx.mqtt.EtxMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.etx.partner.PartnerApiProperties;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientSubType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.EtxClientType;
import us.dot.its.jpo.ode.mec.deposit.models.etx.mqtt.MqttBrokerTarget;
import us.dot.its.jpo.ode.mec.deposit.models.av.mqtt.AvMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.mb.mqtt.MbMqttProperties;
import us.dot.its.jpo.ode.mec.deposit.models.nmi.mqtt.NmiMqttProperties;
import us.dot.its.jpo.asn.j2735.r2024.SPAT.SPAT;

/**
 * Configuration properties for ETX integration. Defines partner API registration, MQTT broker
 * profiles (including TIM/MAP API depositors under the ETX broker), and client configuration.
 */
@Configuration
@ConfigurationProperties(prefix = "mec-deposit.etx")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EtxProperties {
  private boolean enabled;
  private EtxClientType clientType;
  private EtxClientSubType clientSubType;
  private PartnerApiProperties partnerApi;
  @NestedConfigurationProperty
  private MqttBrokers mqttBrokers;

  /**
   * MQTT connection and depositor settings (including TIM/MAP API paths for the ETX broker)
   * grouped by broker.
   */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class MqttBrokers {
    @NestedConfigurationProperty
    private EtxMqttBrokerProfile etx;
    @NestedConfigurationProperty
    private NmiMqttBrokerProfile nmi;
    @NestedConfigurationProperty
    private AvMqttBrokerProfile av;
    @NestedConfigurationProperty
    private MbMqttBrokerProfile mb;
  }

  /** ETX Traffic Exchange MQTT broker profile. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class EtxMqttBrokerProfile {
    @NestedConfigurationProperty
    private EtxMqttProperties mqtt;
    @NestedConfigurationProperty
    private MqttBrokerDepositors depositors;
  }

  /** NMI / TrafficAuth MQTT broker profile. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class NmiMqttBrokerProfile {
    @NestedConfigurationProperty
    private NmiMqttProperties mqtt;
    @NestedConfigurationProperty
    private MqttBrokerDepositors depositors;
  }

  /** AV MQTT broker profile. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class AvMqttBrokerProfile {
    @NestedConfigurationProperty
    private AvMqttProperties mqtt;
    @NestedConfigurationProperty
    private MqttBrokerDepositors depositors;
  }

  /** MB MQTT broker profile. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class MbMqttBrokerProfile {
    @NestedConfigurationProperty
    private MbMqttProperties mqtt;
    @NestedConfigurationProperty
    private MqttBrokerDepositors depositors;
  }

  /**
   * MQTT depositor configuration for one broker (Kafka listeners use the ETX profile's MQTT
   * {@code kafka-topic} settings; keep values in sync when consuming the same topic for both
   * brokers).
   */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class MqttBrokerDepositors {
    private int staleMessageThreshold;
    private DepositorProperties bsm;
    private DepositorProperties psm;
    private DepositorProperties spat;
    private DepositorProperties tim;
    private DepositorProperties map;
    private DepositorProperties sdsm;
    private DepositorProperties geohash;
  }

  /**
   * Properties for ETX depositor configuration groups.
   */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class DepositorProperties {
    private MqttDepositorProperties mqtt;
    private ApiDepositorProperties api;
  }

  /**
   * Properties for the ETX Mqtt configuration.
   */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class MqttDepositorProperties {
    private Boolean enabled;
    private String kafkaTopic;
    private Boolean geofencePreviewEnabled;
    private SpatIntersectionFilterProperties intersectionFilter;
  }

  /**
   * Properties for the SPAT intersection filter configuration.
   */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class SpatIntersectionFilterProperties {
    private Boolean enabled;
    private List<Long> allowedIntersectionIds;
    private List<Long> blockedIntersectionIds;
  }

  /**
   * Properties for the ETX Api configuration.
   */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ApiDepositorProperties {
    private Boolean enabled;
    private String kafkaTopic;
  }

  /**
   * Stale-message threshold in ms: max of ETX/NMI/AV broker depositor settings, default 250.
   */
  public int resolveStaleMessageThresholdMs() {
    int max = 0;
    Optional<MqttBrokerDepositors> etxDeps = optionalEtxBrokerDepositors();
    if (etxDeps.isPresent()) {
      max = Math.max(max, etxDeps.get().getStaleMessageThreshold());
    }
    Optional<MqttBrokerDepositors> nmiDeps = optionalNmiBrokerDepositors();
    if (nmiDeps.isPresent()) {
      max = Math.max(max, nmiDeps.get().getStaleMessageThreshold());
    }
    Optional<MqttBrokerDepositors> avDeps = optionalAvBrokerDepositors();
    if (avDeps.isPresent()) {
      max = Math.max(max, avDeps.get().getStaleMessageThreshold());
    }
    Optional<MqttBrokerDepositors> mbDeps = optionalMbBrokerDepositors();
    if (mbDeps.isPresent()) {
      max = Math.max(max, mbDeps.get().getStaleMessageThreshold());
    }
    return max > 0 ? max : 250;
  }

  /**
   * Whether the given MQTT depositor kind is enabled for the broker (TIM/MAP refer to MQTT path
   * only).
   */
  public boolean isMqttDepositorEnabled(MqttBrokerTarget target, String depositorKind) {
    MqttBrokerDepositors deps = optionalBrokerDepositors(target).orElse(null);
    DepositorProperties p = depositorPropertiesForKind(deps, depositorKind);
    return p != null && p.getMqtt() != null && Boolean.TRUE.equals(p.getMqtt().getEnabled());
  }

  /**
   * Precision for NMI topic construction; defaults to 7 when unset.
   */
  public int mqttTopicPrecision(MqttBrokerTarget target) {
    if (mqttBrokers == null) {
      return 7;
    }
    if (target == MqttBrokerTarget.NMI && mqttBrokers.getNmi() != null
        && mqttBrokers.getNmi().getMqtt() != null) {
      return mqttBrokers.getNmi().getMqtt().getPrecision();
    }
    if (target == MqttBrokerTarget.AV && mqttBrokers.getAv() != null
        && mqttBrokers.getAv().getMqtt() != null) {
      return mqttBrokers.getAv().getMqtt().getPrecision();
    }
    if (target == MqttBrokerTarget.MB && mqttBrokers.getMb() != null
        && mqttBrokers.getMb().getMqtt() != null) {
      return mqttBrokers.getMb().getMqtt().getPrecision();
    }
    return 7;
  }

  /**
   * Backward-compatible helper for existing tests/callers.
   */
  public int nmiMqttTopicPrecision() {
    return mqttTopicPrecision(MqttBrokerTarget.NMI);
  }

  /**
   * When true, TIM MQTT depositors resolve publish topics from Partner API geofence preview
   * geohashes instead of TIM data-frame region anchors.
   */
  public boolean isTimMqttGeofencePreviewEnabled() {
    return optionalEtxBrokerDepositors()
        .map(MqttBrokerDepositors::getTim)
        .map(DepositorProperties::getMqtt)
        .map(MqttDepositorProperties::getGeofencePreviewEnabled)
        .map(Boolean.TRUE::equals)
        .orElse(false);
  }

  /**
   * SPAT intersection filter evaluation for a specific broker profile.
   */
  public boolean passesSpatIntersectionFilter(MqttBrokerTarget target, SPAT spatMsg) {
    MqttBrokerDepositors deps = optionalBrokerDepositors(target).orElse(null);
    if (deps == null || deps.getSpat() == null || deps.getSpat().getMqtt() == null) {
      return false;
    }
    if (!Boolean.TRUE.equals(deps.getSpat().getMqtt().getEnabled())) {
      return false;
    }
    SpatIntersectionFilterProperties filter = deps.getSpat().getMqtt().getIntersectionFilter();
    if (filter == null || !Boolean.TRUE.equals(filter.getEnabled())) {
      return true;
    }
    List<Long> allowedIntersectionIds = filter.getAllowedIntersectionIds();
    List<Long> blockedIntersectionIds = filter.getBlockedIntersectionIds();

    if (spatMsg.getIntersections() != null && !spatMsg.getIntersections().isEmpty()) {
      Long intersectionId = spatMsg.getIntersections().get(0).getId().getId().getValue();
      if (blockedIntersectionIds != null && blockedIntersectionIds.contains(intersectionId)) {
        return false;
      }
      if (allowedIntersectionIds == null || allowedIntersectionIds.isEmpty()) {
        return true;
      }
      return allowedIntersectionIds.contains(intersectionId);
    }
    return false;
  }

  private Optional<MqttBrokerDepositors> optionalEtxBrokerDepositors() {
    if (mqttBrokers == null || mqttBrokers.getEtx() == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(mqttBrokers.getEtx().getDepositors());
  }

  private Optional<MqttBrokerDepositors> optionalNmiBrokerDepositors() {
    if (mqttBrokers == null || mqttBrokers.getNmi() == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(mqttBrokers.getNmi().getDepositors());
  }

  private Optional<MqttBrokerDepositors> optionalAvBrokerDepositors() {
    if (mqttBrokers == null || mqttBrokers.getAv() == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(mqttBrokers.getAv().getDepositors());
  }

  private Optional<MqttBrokerDepositors> optionalMbBrokerDepositors() {
    if (mqttBrokers == null || mqttBrokers.getMb() == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(mqttBrokers.getMb().getDepositors());
  }

  private Optional<MqttBrokerDepositors> optionalBrokerDepositors(MqttBrokerTarget target) {
    if (target == null) {
      return Optional.empty();
    }
    return switch (target) {
      case ETX -> optionalEtxBrokerDepositors();
      case NMI -> optionalNmiBrokerDepositors();
      case AV -> optionalAvBrokerDepositors();
      case MB -> optionalMbBrokerDepositors();
    };
  }

  private static DepositorProperties depositorPropertiesForKind(MqttBrokerDepositors d,
      String kind) {
    if (d == null || kind == null) {
      return null;
    }
    return switch (kind) {
      case "bsm" -> d.getBsm();
      case "psm" -> d.getPsm();
      case "spat" -> d.getSpat();
      case "tim" -> d.getTim();
      case "map" -> d.getMap();
      case "sdsm" -> d.getSdsm();
      case "geohash" -> d.getGeohash();
      default -> null;
    };
  }
}
