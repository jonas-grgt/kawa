package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class UpdateTopicHandlerTest {

    @Test
    void putAddsVirtualTopicAndPersistsSnapshot() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty());
        var handler = new UpdateTopicHandler(repository);
        byte[] body = "{\"type\":\"virtual\",\"topic\":\"raw-orders\"}".getBytes(StandardCharsets.UTF_8);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/topics/orders", Map.of("name", "orders"), body));

        // then
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.body()).isEqualTo(new VirtualTopicConfig("raw-orders"));
        assertThat(repository.current().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("raw-orders"));
    }

    @Test
    void putOverwritesExistingEntry() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .putVirtualTopic("orders", new VirtualTopicConfig("raw-old")));
        var handler = new UpdateTopicHandler(repository);
        byte[] body = "{\"type\":\"virtual\",\"topic\":\"raw-new\"}".getBytes(StandardCharsets.UTF_8);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/topics/orders", Map.of("name", "orders"), body));

        // then
        assertThat(response.status()).isEqualTo(200);
        assertThat(repository.current().virtualTopics()).hasSize(1);
        assertThat(repository.current().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("raw-new"));
    }

    @Test
    void putRejectsInvalidBody() {
        // given
        var handler = new UpdateTopicHandler(new FakeGatewayConfigRepository(GatewayConfig.empty()));

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/topics/orders", Map.of("name", "orders"),
                        "not json".getBytes(StandardCharsets.UTF_8)));

        // then
        assertThat(response.status()).isEqualTo(400);
    }

    @Test
    void putRejectsPhysicalType() {
        // given
        var handler = new UpdateTopicHandler(new FakeGatewayConfigRepository(GatewayConfig.empty()));

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/topics/orders", Map.of("name", "orders"),
                        "{\"type\":\"physical\",\"partitions\":5}".getBytes(StandardCharsets.UTF_8)));

        // then
        assertThat(response.status()).isEqualTo(400);
    }

    @Test
    void putRejectsVirtualWithoutBacking() {
        // given
        var handler = new UpdateTopicHandler(new FakeGatewayConfigRepository(GatewayConfig.empty()));

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/topics/orders", Map.of("name", "orders"),
                        "{\"type\":\"virtual\"}".getBytes(StandardCharsets.UTF_8)));

        // then
        assertThat(response.status()).isEqualTo(400);
    }
}