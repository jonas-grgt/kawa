package io.jonasg.kawa.rbac;

import java.util.ArrayList;
import java.util.List;

/// Per-request state for CreatePartitions authorization: remembers which specifically-named topics
/// were denied, so the response can be reconstructed with a TOPIC_AUTHORIZATION_FAILED entry per
/// denied topic once the broker replies for the authorized ones.
final class CreatePartitionsAuthState {

    private final List<String> deniedTopics = new ArrayList<>();

    void recordDenied(List<String> topics) {
        deniedTopics.addAll(topics);
    }

    List<String> deniedTopics() {
        return List.copyOf(deniedTopics);
    }
}
