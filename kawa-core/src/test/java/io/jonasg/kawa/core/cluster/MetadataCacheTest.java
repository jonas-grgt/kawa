package io.jonasg.kawa.core.cluster;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MetadataCacheTest {

    private final MetadataCache cache = new MetadataCache();

    private MetadataSnapshot snapshot() {
        TopicMetadata orders = TopicMetadata.of("orders-v2", List.of(
                PartitionMetadata.of(0, 1, List.of(1), List.of(1), List.of()),
                PartitionMetadata.of(1, 2, List.of(2), List.of(2), List.of())));
        TopicMetadata replicated = TopicMetadata.of("replicated", List.of(
                PartitionMetadata.of(0, 1, List.of(1, 2), List.of(1, 2), List.of())));
        return MetadataSnapshot.of(
                Map.of("orders-v2", orders, "replicated", replicated),
                Map.of(1, BrokerNode.of(1, "broker-1", 9093, null), 2, BrokerNode.of(2, "broker-2", 9093, null)),
                "cluster-1");
    }

    @Test
    void returnsLeaderPerPartition() {
        cache.update(snapshot());

        assertThat(cache.leaderFor("orders-v2", 0)).isEqualTo(1);
        assertThat(cache.leaderFor("orders-v2", 1)).isEqualTo(2);
        assertThat(cache.leaderFor("orders-v2", 7)).isEqualTo(-1);
        assertThat(cache.leaderFor("unknown", 0)).isEqualTo(-1);
    }

    @Test
    void returnsAnyLeader() {
        cache.update(snapshot());

        assertThat(cache.leaderForAny("orders-v2")).isEqualTo(1);
        assertThat(cache.leaderForAny("unknown")).isEqualTo(-1);
    }

    @Test
    void returnsBrokerNode() {
        cache.update(snapshot());

        assertThat(cache.broker(2)).isEqualTo(BrokerNode.of(2, "broker-2", 9093, null));
        assertThat(cache.broker(99)).isNull();
    }

    @Test
    void returnsReplicationFactor() {
        cache.update(snapshot());

        assertThat(cache.replicationFactor("orders-v2")).isEqualTo(1);
        assertThat(cache.replicationFactor("replicated")).isEqualTo(2);
        assertThat(cache.replicationFactor("unknown")).isZero();
    }
}
