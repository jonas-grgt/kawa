package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import java.util.function.UnaryOperator;

/// In-memory [GatewayConfigRepository] for handler tests: [upsert] replaces the current
/// snapshot synchronously, mirroring what the config-topic consumer does asynchronously.
final class FakeGatewayConfigRepository implements GatewayConfigRepository {

    private GatewayConfig current;

    FakeGatewayConfigRepository(GatewayConfig initial) {
        this.current = initial;
    }

    @Override
    public GatewayConfig getActiveConfig() {
        return current;
    }

    @Override
    public void upsert(GatewayConfig config) {
        this.current = config;
    }

    @Override
    public void update(UnaryOperator<GatewayConfig> mutation) {
        upsert(mutation.apply(getActiveConfigOrEmpty()));
    }

    @Override
    public void close() {
        // no resources
    }
}