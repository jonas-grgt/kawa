package io.jonasg.kawa.rbac;

import java.util.ArrayList;
import java.util.List;

/// Per-request state for Metadata authorization: remembers which specifically-named topics were
/// denied, so the response can be reconstructed with an UNKNOWN_TOPIC_OR_PARTITION entry per
/// denied topic once the broker replies for the authorized ones.
final class MetadataAuthState {

    private final List<String> deniedTopics = new ArrayList<>();

    void recordDenied(String topic) {
        deniedTopics.add(topic);
    }

    List<String> deniedTopics() {
        return List.copyOf(deniedTopics);
    }
}
