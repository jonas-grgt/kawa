package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.core.cluster.BrokerNode;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.cluster.MetadataSnapshot;
import io.jonasg.kawa.core.cluster.PartitionMetadata;
import io.jonasg.kawa.core.cluster.TopicMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DeleteTopicHandlerTest {

    @Test
    void deleteRemovesVirtualTopicConfigWithoutBrokerCall() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .putVirtualTopic("orders", new VirtualTopicConfig("orders-v2")));
        var topicAdmin = new FakeTopicAdmin();
        var handler = new DeleteTopicHandler(repository, new MetadataCache(), topicAdmin);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("DELETE", "/topics/orders", Map.of("name", "orders"), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(204);
        assertThat(repository.current().virtualTopics()).isEmpty();
        assertThat(topicAdmin.deleted).isEmpty();
    }

    @Test
    void deleteDeletesPhysicalTopicOnBroker() {
        // given
        var cache = new MetadataCache();
        cache.update(MetadataSnapshot.of(
                Map.of("orders", TopicMetadata.of("orders",
                        List.of(PartitionMetadata.of(0, 1, List.of(1), List.of(1), List.of())))),
                Map.of(1, BrokerNode.of(1, "localhost", 9092, null)),
                "test-cluster"));
        var topicAdmin = new FakeTopicAdmin();
        var handler = new DeleteTopicHandler(
                new FakeGatewayConfigRepository(GatewayConfig.empty()), cache, topicAdmin);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("DELETE", "/topics/orders", Map.of("name", "orders"), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(204);
        assertThat(topicAdmin.deleted).containsExactly("orders");
    }

    @Test
    void deleteUnknownTopicReturnsNotFound() {
        // given
        var handler = new DeleteTopicHandler(
                new FakeGatewayConfigRepository(GatewayConfig.empty()), new MetadataCache(), new FakeTopicAdmin());

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("DELETE", "/topics/orders", Map.of("name", "orders"), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(404);
    }

    @Test
    void deleteVirtualTopicWinsOverPhysicalName() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .putVirtualTopic("orders", new VirtualTopicConfig("orders-v2")));
        var cache = new MetadataCache();
        cache.update(MetadataSnapshot.of(
                Map.of("orders", TopicMetadata.of("orders",
                        List.of(PartitionMetadata.of(0, 1, List.of(1), List.of(1), List.of())))),
                Map.of(1, BrokerNode.of(1, "localhost", 9092, null)),
                "test-cluster"));
        var topicAdmin = new FakeTopicAdmin();
        var handler = new DeleteTopicHandler(repository, cache, topicAdmin);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("DELETE", "/topics/orders", Map.of("name", "orders"), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(204);
        assertThat(repository.current().virtualTopics()).isEmpty();
        assertThat(topicAdmin.deleted).isEmpty();
    }
}