package io.jonasg.kawa.rbac;

import java.util.LinkedHashSet;
import java.util.Set;

/// Per-request state for DescribeConfigs authorization: remembers which TOPIC-typed resource
/// names were denied, so the response can be reconstructed with a TOPIC_AUTHORIZATION_FAILED
/// result per denied resource once the broker replies for the authorized ones. Non-TOPIC
/// resources (BROKER, BROKER_LOGGER) are never gated and never recorded here.
final class DescribeConfigsAuthState {

    private final Set<String> deniedResourceNames = new LinkedHashSet<>();

    void recordDenied(String resourceName) {
        deniedResourceNames.add(resourceName);
    }

    Set<String> deniedResourceNames() {
        return Set.copyOf(deniedResourceNames);
    }
}
