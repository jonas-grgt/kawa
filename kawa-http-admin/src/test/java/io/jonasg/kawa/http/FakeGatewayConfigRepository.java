package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;

/// In-memory [GatewayConfigRepository] for handler tests: [write] replaces the current
/// snapshot synchronously, mirroring what the config-topic consumer does asynchronously.
final class FakeGatewayConfigRepository implements GatewayConfigRepository {

    private GatewayConfig current;

    FakeGatewayConfigRepository(GatewayConfig initial) {
        this.current = initial;
    }

    @Override
    public GatewayConfig current() {
        return current;
    }

    @Override
    public void write(GatewayConfig config) {
        this.current = config;
    }

    @Override
    public void close() {
        // no resources
    }
}