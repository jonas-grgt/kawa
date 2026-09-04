package io.jonasg.kawa.rbac;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Per-request state for DeleteRecords authorization: remembers which topics were denied and the
/// partition indices the client sent for each, so the response can be reconstructed with a
/// TOPIC_AUTHORIZATION_FAILED entry per denied partition once the broker replies for the
/// authorized topics.
final class DeleteRecordsAuthState {

    private final Map<String, List<Integer>> deniedPartitionsByTopic = new LinkedHashMap<>();

    void recordDenied(String topic, List<Integer> partitions) {
        deniedPartitionsByTopic.put(topic, List.copyOf(partitions));
    }

    Map<String, List<Integer>> deniedPartitionsByTopic() {
        return Map.copyOf(deniedPartitionsByTopic);
    }
}
