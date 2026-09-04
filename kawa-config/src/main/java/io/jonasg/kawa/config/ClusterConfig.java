package io.jonasg.kawa.config;

import java.util.List;

/// A Kafka cluster the gateway can forward traffic to.
///
/// @param name cluster name (immutable, defaults to the config map key)
/// @param bootstrapServers `host:port` list used to open the initial broker connection
public record ClusterConfig(String name, List<String> bootstrapServers) {

    public ClusterConfig {
        if (bootstrapServers == null) {
            bootstrapServers = List.of();
        }
    }

    public static ClusterConfig of(
            String name,
            List<String> bootstrapServers
    ) {
        return new ClusterConfig(name, bootstrapServers);
    }
}
