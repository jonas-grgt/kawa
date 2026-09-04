package io.jonasg.kawa.rbac;

import java.util.LinkedHashSet;
import java.util.Set;

/// Per-request state for OffsetFetch authorization: remembers which topics were denied on the
/// per-topic TOPIC DESCRIBE gate, so the response can be reconstructed with a
/// TOPIC_AUTHORIZATION_FAILED entry per denied topic once the broker replies for the authorized
/// ones. Only used in the non-null `topics()` case - when `topics()` is null (fetch-all) the
/// request is forwarded unchanged. The GROUP DESCRIBE gate never needs state - when it fails the
/// whole request is short-circuited immediately.
final class OffsetFetchAuthState {

    private final Set<String> deniedTopics = new LinkedHashSet<>();

    void recordDenied(String topic) {
        deniedTopics.add(topic);
    }

    Set<String> deniedTopics() {
        return Set.copyOf(deniedTopics);
    }
}
