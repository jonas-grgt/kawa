package io.jonasg.kawa.virtualtopic;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class FetchSessionRegistryTest {

    private final Object client = new Object();
    private final FetchSessionRegistry registry = new FetchSessionRegistry();

    @Test
    void bindsSessionAndResolvesLogicalNames() {
        registry.bindSession(client, 1, Map.of("customers-v2", "customers"));

        assertThat(registry.hasSession(client, 1)).isTrue();
        assertThat(registry.logicalFor(client, 1, "customers-v2")).isEqualTo("customers");
        assertThat(registry.logicalFor(client, 1, "orders-v2")).isNull();
    }

    @Test
    void ignoresBindForZeroSessionIdOrEmptyMapping() {
        registry.bindSession(client, 0, Map.of("customers-v2", "customers"));
        registry.bindSession(client, 2, Map.of());

        assertThat(registry.hasSession(client, 0)).isFalse();
        assertThat(registry.hasSession(client, 2)).isFalse();
    }

    @Test
    void mergesAndForgetsOnFetchRequest() {
        registry.bindSession(client, 1, Map.of("customers-v2", "customers"));

        registry.onFetchRequest(client, 1, Map.of("orders-v2", "orders"), List.of());
        assertThat(registry.logicalFor(client, 1, "orders-v2")).isEqualTo("orders");

        registry.onFetchRequest(client, 1, Map.of(), List.of("customers-v2"));
        assertThat(registry.logicalFor(client, 1, "customers-v2")).isNull();
    }

    @Test
    void removesSessionAndDropsAllOnDisconnect() {
        registry.bindSession(client, 1, Map.of("customers-v2", "customers"));
        registry.removeSession(client, 1);
        assertThat(registry.hasSession(client, 1)).isFalse();

        registry.bindSession(client, 1, Map.of("customers-v2", "customers"));
        registry.bindSession(client, 2, Map.of("orders-v2", "orders"));
        registry.sessionClosed(client);
        assertThat(registry.hasSession(client, 1)).isFalse();
        assertThat(registry.hasSession(client, 2)).isFalse();
    }

    @Test
    void sessionsAreScopedPerClient() {
        registry.bindSession(client, 1, Map.of("customers-v2", "customers"));

        assertThat(registry.hasSession(new Object(), 1)).isFalse();
    }
}
