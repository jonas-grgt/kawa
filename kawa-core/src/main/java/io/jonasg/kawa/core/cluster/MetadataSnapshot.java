package io.jonasg.kawa.core.cluster;

import java.util.Map;

/// @param topics physical topic metadata keyed by physical name
/// @param brokers physical broker nodes keyed by broker id
/// @param clusterId Kafka cluster id
public record MetadataSnapshot(
        Map<String, TopicMetadata> topics,
        Map<Integer, BrokerNode> brokers,
        String clusterId) {

    public static MetadataSnapshot of(
            Map<String, TopicMetadata> topics,
            Map<Integer, BrokerNode> brokers,
            String clusterId) {
        return new MetadataSnapshot(topics, brokers, clusterId);
    }
}
