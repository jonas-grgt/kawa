package io.jonasg.kawa.config;

import tools.jackson.databind.annotation.JsonDeserialize;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/// Immutable gateway configuration.
///
/// @param name gateway name
/// @param listeners client-facing listeners
/// @param clusters upstream Kafka clusters
/// @param virtualTopics logical virtual topic config map
/// @param advertised the endpoint advertised to clients in rewritten metadata
/// @param metrics observability settings
/// @param auth client SASL authentication configuration
/// @param rbac role-based access control configuration
/// @param admin admin HTTP listener configuration
public record GatewayConfig(
        String name,
        List<ListenerConfig> listeners,
        Map<String, ClusterConfig> clusters,
        @JsonDeserialize(using = VirtualTopicMapDeserializer.class) Map<String, VirtualTopicConfig> virtualTopics,
        AdvertisedListener advertised,
        MetricsConfig metrics,
        AuthConfig auth,
        RbacConfig rbac,
        AdminConfig admin) {

    public GatewayConfig {
        if (name == null) {
            name = "kafka-gateway";
        }
        if (listeners == null) {
            listeners = List.of(new ListenerConfig(null, 9092));
        }
        if (clusters == null) {
            clusters = Map.of();
        }
        if (virtualTopics == null) {
            virtualTopics = Map.of();
        }
        if (metrics == null) {
            metrics = new MetricsConfig(false, null);
        }
        if (auth == null) {
            auth = new AuthConfig(null, null, null);
        }
        if (rbac == null) {
            rbac = new RbacConfig(null, null);
        }
        if (admin == null) {
            admin = new AdminConfig(false, null, null, null);
        }
        if (advertised == null) {
            ListenerConfig first = listeners.getFirst();
            advertised = new AdvertisedListener(null, null, first.port());
        }
    }

    /// Convenience factory for programmatic configuration.
    public static GatewayConfig of(
            String name,
            List<ListenerConfig> listeners,
            Map<String, ClusterConfig> clusters,
            Map<String, String> virtualTopics,
            AdvertisedListener advertised,
            MetricsConfig metrics,
            AuthConfig auth) {
        Map<String, VirtualTopicConfig> typedVirtualTopics = virtualTopics == null
                ? null
                : virtualTopics.entrySet().stream()
                        .collect(Collectors.toUnmodifiableMap(
                                Map.Entry::getKey,
                                entry -> new VirtualTopicConfig(entry.getValue())));
        return new GatewayConfig(name, listeners, clusters, typedVirtualTopics, advertised, metrics, auth, null, null);
    }

    /// The default cluster (first entry), or `null` if none is configured.
    public ClusterConfig defaultCluster() {
        if (clusters.isEmpty()) {
            return null;
        }
        return clusters.values().iterator().next();
    }
}
