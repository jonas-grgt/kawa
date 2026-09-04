package io.jonasg.kawa.server;

import io.jonasg.kawa.core.Request;
import io.jonasg.kawa.core.Route;
import io.jonasg.kawa.core.cluster.BrokerNode;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.cluster.MetadataSnapshot;
import io.jonasg.kawa.core.cluster.PartitionMetadata;
import io.jonasg.kawa.core.cluster.TopicMetadata;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class LeaderRouterTest {

    private MetadataCache cacheWithTopology() {
        var cache = new MetadataCache();
        MetadataSnapshot snapshot = MetadataSnapshot.of(
                Map.of(
                        "orders-v2", TopicMetadata.of("orders-v2", List.of(
                                PartitionMetadata.of(0, 1, List.of(1), List.of(1), List.of()),
                                PartitionMetadata.of(1, 2, List.of(2), List.of(2), List.of()))),
                        "customers-v2", TopicMetadata.of("customers-v2", List.of(
                                PartitionMetadata.of(0, 2, List.of(2), List.of(2), List.of()))),
                        "__consumer_offsets", TopicMetadata.of("__consumer_offsets", List.of(
                                PartitionMetadata.of(0, 1, List.of(1), List.of(1), List.of()),
                                PartitionMetadata.of(1, 2, List.of(2), List.of(2), List.of())))),
                Map.of(
                        1, BrokerNode.of(1, "broker-1", 9092, null),
                        2, BrokerNode.of(2, "broker-2", 9093, null)),
                "cluster-1");
        cache.update(snapshot);
        return cache;
    }

    @Test
    void routesProduceToPartitionLeader() {
        var router = new LeaderRouter(cacheWithTopology());

        var data = new ProduceRequestData();
        data.topicData().add(new ProduceRequestData.TopicProduceData().setName("orders-v2")
                .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData().setIndex(1))));

        Route route = router.route(request(0, data));
        assertThat(route.brokerId()).isEqualTo(2);
    }

    @Test
    void routesToAnyBrokerForPassthroughRequests() {
        var router = new LeaderRouter(cacheWithTopology());

        Route route = router.route(request(25, null));
        assertThat(route.brokerId()).isEqualTo(1);
    }

    @Test
    void returnsUnroutedWhenTopologyUnknown() {
        var router = new LeaderRouter(new MetadataCache());

        Route route = router.route(request(25, null));
        assertThat(route).isEqualTo(Route.UNROUTED);
    }

    @Test
    void offsetApisShouldRouteByGroupNotByTopicLeader() {
        var router = new LeaderRouter(cacheWithTopology());

        var fetchOnOrders = new OffsetFetchRequestData().setGroupId("group-a");
        fetchOnOrders.topics().add(new OffsetFetchRequestData.OffsetFetchRequestTopic().setName("orders-v2"));
        var fetchOnCustomers = new OffsetFetchRequestData().setGroupId("group-a");
        fetchOnCustomers.topics().add(new OffsetFetchRequestData.OffsetFetchRequestTopic().setName("customers-v2"));

        Route fetchOrdersRoute = router.route(request(9, fetchOnOrders));
        Route fetchCustomersRoute = router.route(request(9, fetchOnCustomers));

        assertThat(fetchOrdersRoute.brokerId())
                .describedAs("same group should route to one coordinator regardless of data topic")
                .isEqualTo(fetchCustomersRoute.brokerId());

        var commitOnOrders = new OffsetCommitRequestData().setGroupId("group-a");
        commitOnOrders.topics().add(new OffsetCommitRequestData.OffsetCommitRequestTopic().setName("orders-v2"));
        var commitOnCustomers = new OffsetCommitRequestData().setGroupId("group-a");
        commitOnCustomers.topics().add(new OffsetCommitRequestData.OffsetCommitRequestTopic().setName("customers-v2"));

        Route commitOrdersRoute = router.route(request(8, commitOnOrders));
        Route commitCustomersRoute = router.route(request(8, commitOnCustomers));

        assertThat(commitOrdersRoute.brokerId())
                .describedAs("same group should route to one coordinator regardless of data topic")
                .isEqualTo(commitCustomersRoute.brokerId());
    }

    private static Request request(
            int apiKey,
            Object body
    ) {
        return new Request() {
            @Override
            public int apiKey() {
                return apiKey;
            }

            @Override
            public String apiName() {
                return "test";
            }

            @Override
            public short apiVersion() {
                return 8;
            }

            @Override
            public int correlationId() {
                return 1;
            }

            @Override
            public String clientId() {
                return "test";
            }

            @Override
            public Object body() {
                return body;
            }
        };
    }
}
