package io.jonasg.kawa.governance;

import java.util.Map;

/// A topic creation request as seen by governance: the name, requested partitions and
/// replication factor, and topic configs. A `-1` partitions or replication factor means the
/// client left it to the broker default.
///
/// @param name the topic name
/// @param partitions requested partitions, or `-1` for the broker default
/// @param replicationFactor requested replication factor, or `-1` for the broker default
/// @param configs topic configs, keyed by config name
public record TopicSpec(
        String name,
        int partitions,
        int replicationFactor,
        Map<String, String> configs
) {

    public TopicSpec {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be null or blank");
        }
        configs = configs == null ? Map.of() : Map.copyOf(configs);
    }

    /// Whether the client explicitly requested a partition count (as opposed to the broker default).
    public boolean partitionsSpecified() {
        return partitions != -1;
    }

    /// Whether the client explicitly requested a replication factor (as opposed to the broker default).
    public boolean replicationFactorSpecified() {
        return replicationFactor != -1;
    }
}