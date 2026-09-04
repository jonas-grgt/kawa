package io.jonasg.kawa.rbac;

import java.util.ArrayList;
import java.util.List;

/// Per-request state for DescribeGroups authorization: remembers which specifically-named groups
/// were denied, so the response can be reconstructed with a GROUP_AUTHORIZATION_FAILED entry per
/// denied group once the broker replies for the authorized ones.
final class DescribeGroupsAuthState {

    private final List<String> deniedGroups = new ArrayList<>();

    void recordDenied(List<String> groups) {
        deniedGroups.addAll(groups);
    }

    List<String> deniedGroups() {
        return List.copyOf(deniedGroups);
    }
}
