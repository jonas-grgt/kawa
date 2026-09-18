package io.jonasg.kawa.config;

import java.util.List;

/// A Kafka cluster the gateway can forward traffic to.
///
/// @param bootstrapServers `host:port` list used to open the initial broker connection
public record ClusterConfig(List<String> bootstrapServers) {

    public ClusterConfig {
        if (bootstrapServers == null) {
            bootstrapServers = List.of();
        }
    }

    public static ClusterConfig of(
            List<String> bootstrapServers
    ) {
        return new ClusterConfig(bootstrapServers);
    }
}
