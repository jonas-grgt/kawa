package io.jonasg.kawa.rbac;

import java.util.ArrayList;
import java.util.List;

/// Per-request state for DescribeTransactions authorization: remembers which specifically-named
/// transaction ids were denied, so the response can be reconstructed with a
/// TRANSACTIONAL_ID_AUTHORIZATION_FAILED entry per denied id once the broker replies for the
/// authorized ones.
final class DescribeTransactionsAuthState {

    private final List<String> deniedTransactionalIds = new ArrayList<>();

    void recordDenied(List<String> ids) {
        deniedTransactionalIds.addAll(ids);
    }

    List<String> deniedTransactionalIds() {
        return List.copyOf(deniedTransactionalIds);
    }
}
