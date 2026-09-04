package io.jonasg.kawa.http;

import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.core.VirtualTopicManager;
import io.jonasg.kawa.core.cluster.BrokerNode;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.cluster.MetadataSnapshot;
import io.jonasg.kawa.core.cluster.PartitionMetadata;
import io.jonasg.kawa.core.cluster.TopicMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TopicsHandlerTest {

    @Test
    void returnsJoinedLogicalAndPhysicalTopics() {
        // given
        var virtualTopics = new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2"),
                "customers", new VirtualTopicConfig("crm.customers",
                        new HeaderEqualsFilterConfig("tenant", "acme"), true)));
        MetadataCache cache = cacheWith(
                topic("orders-v2", 3),
                topic("crm.customers", 2),
                topic("raw-events", 1));
        var handler = new TopicsHandler(virtualTopics, cache);

        // when
        List<TopicView> topics = handler.handle();

        // then
        assertThat(topics).containsExactlyInAnyOrder(
                new TopicView("orders", "orders-v2", 3, null, false),
                new TopicView("customers", "crm.customers", 2, "headerEquals(tenant=acme)", true),
                new TopicView(null, "raw-events", 1, null, false));
    }

    @Test
    void returnsEmptyListWhenNoTopics() {
        // given
        var handler = new TopicsHandler(new VirtualTopicManager(Map.of()), new MetadataCache());

        // when
        List<TopicView> topics = handler.handle();

        // then
        assertThat(topics).isEmpty();
    }

    @Test
    void describesCelFilter() {
        // given
        var virtualTopics = new VirtualTopicManager(Map.of(
                "audit", new VirtualTopicConfig("audit-v1",
                        new CelFilterConfig("headers.tenant == \"acme\""), false)));
        MetadataCache cache = cacheWith(topic("audit-v1", 1));
        var handler = new TopicsHandler(virtualTopics, cache);

        // when
        List<TopicView> topics = handler.handle();

        // then
        assertThat(topics).singleElement().satisfies(view ->
                assertThat(view.filter()).isEqualTo("cel(headers.tenant == \"acme\")"));
    }

    private static MetadataCache cacheWith(TopicMetadata... topics) {
        var cache = new MetadataCache();
        Map<String, TopicMetadata> topicMap = new java.util.HashMap<>();
        for (TopicMetadata topic : topics) {
            topicMap.put(topic.name(), topic);
        }
        cache.update(MetadataSnapshot.of(
                topicMap,
                Map.of(1, BrokerNode.of(1, "localhost", 9092, null)),
                "test-cluster"));
        return cache;
    }

    private static TopicMetadata topic(String name, int partitions) {
        var partitionList = new java.util.ArrayList<PartitionMetadata>();
        for (int i = 0; i < partitions; i++) {
            partitionList.add(PartitionMetadata.of(i, 1, List.of(1), List.of(1), List.of()));
        }
        return TopicMetadata.of(name, partitionList);
    }
}
