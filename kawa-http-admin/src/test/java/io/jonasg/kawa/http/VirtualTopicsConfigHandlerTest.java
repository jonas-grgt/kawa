package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class VirtualTopicsConfigHandlerTest {

    @Test
    void getListsConfiguredVirtualTopics() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .putVirtualTopic("orders", new VirtualTopicConfig("raw-orders")));
        var handler = new VirtualTopicsConfigHandler(repository);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("GET", "/config/virtual-topics", Map.of(), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.body()).isEqualTo(Map.of("orders", new VirtualTopicConfig("raw-orders")));
    }

    @Test
    void getReturnsEmptyMapWhenNoSnapshotApplied() {
        // given
        var handler = new VirtualTopicsConfigHandler(new FakeGatewayConfigRepository(null));

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("GET", "/config/virtual-topics", Map.of(), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.body()).isEqualTo(Map.of());
    }

    @Test
    void putAddsVirtualTopicAndPersistsSnapshot() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty());
        var handler = new VirtualTopicsConfigHandler(repository);
        byte[] body = "{\"topic\":\"raw-orders\"}".getBytes(StandardCharsets.UTF_8);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/config/virtual-topics/orders", Map.of("name", "orders"), body));

        // then
        assertThat(response.status()).isEqualTo(200);
        assertThat(repository.current().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("raw-orders"));
    }

    @Test
    void putOverwritesExistingEntry() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .putVirtualTopic("orders", new VirtualTopicConfig("raw-old")));
        var handler = new VirtualTopicsConfigHandler(repository);
        byte[] body = "{\"topic\":\"raw-new\"}".getBytes(StandardCharsets.UTF_8);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/config/virtual-topics/orders", Map.of("name", "orders"), body));

        // then
        assertThat(response.status()).isEqualTo(200);
        assertThat(repository.current().virtualTopics()).hasSize(1);
        assertThat(repository.current().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("raw-new"));
    }

    @Test
    void putRejectsInvalidBody() {
        // given
        var handler = new VirtualTopicsConfigHandler(new FakeGatewayConfigRepository(GatewayConfig.empty()));

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/config/virtual-topics/orders", Map.of("name", "orders"),
                        "not json".getBytes(StandardCharsets.UTF_8)));

        // then
        assertThat(response.status()).isEqualTo(400);
    }

    @Test
    void deleteRemovesEntryAndPersistsSnapshot() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .putVirtualTopic("orders", new VirtualTopicConfig("raw-orders")));
        var handler = new VirtualTopicsConfigHandler(repository);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("DELETE", "/config/virtual-topics/orders", Map.of("name", "orders"), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(204);
        assertThat(repository.current().virtualTopics()).isEmpty();
    }

    @Test
    void deleteMissingEntryReturnsNotFound() {
        // given
        var handler = new VirtualTopicsConfigHandler(new FakeGatewayConfigRepository(GatewayConfig.empty()));

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("DELETE", "/config/virtual-topics/orders", Map.of("name", "orders"), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(404);
    }
}