package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;

import java.util.function.UnaryOperator;

final class ConsistencyAwareUpdater {

    private final GatewayConfigRepository repository;

    ConsistencyAwareUpdater(GatewayConfigRepository repository) {
        this.repository = repository;
    }

    void update(
            Router.Request request,
            UnaryOperator<GatewayConfig> mutation
    ) {
        String consistency = request.queryParams().get("consistency");
        if (consistency == null || "persisted".equals(consistency)) {
            repository.update(mutation);
            return;
        }
        if ("applied".equals(consistency)) {
            repository.updateAndWaitUntilApplied(mutation);
            return;
        }
        throw new IllegalArgumentException(
                "invalid consistency '" + consistency + "' (expected 'persisted' or 'applied')");
    }
}
