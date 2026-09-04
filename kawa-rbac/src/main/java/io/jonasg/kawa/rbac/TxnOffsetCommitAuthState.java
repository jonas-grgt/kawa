package io.jonasg.kawa.rbac;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Per-request state for TxnOffsetCommit authorization: remembers which topics were denied on the
/// per-topic TOPIC READ gate, so the response can be reconstructed with a
/// TOPIC_AUTHORIZATION_FAILED entry per denied partition once the broker replies for the
/// authorized topics. The TRANSACTIONAL_ID WRITE and GROUP READ gates never need state - when
/// either fails the whole request is short-circuited immediately.
final class TxnOffsetCommitAuthState {

    private final Map<String, List<Integer>> deniedPartitionsByTopic = new LinkedHashMap<>();

    void recordDenied(String topic, List<Integer> partitions) {
        deniedPartitionsByTopic.put(topic, List.copyOf(partitions));
    }

    Map<String, List<Integer>> deniedPartitionsByTopic() {
        return Map.copyOf(deniedPartitionsByTopic);
    }
}
