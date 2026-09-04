package io.jonasg.kawa.server;

import io.jonasg.kawa.core.Request;
import io.jonasg.kawa.core.Route;
import io.jonasg.kawa.core.cluster.BrokerNode;
import io.jonasg.kawa.core.cluster.MetadataCache;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.utils.Utils;

/// Routes each request to the physical broker leading its first partition. Requests that
/// span multiple brokers are sent to the first leader; the broker replies with
/// NOT_LEADER_OR_FENCED for the others and the client retries, converging on the correct
/// brokers. Requests without topic data (Metadata, ApiVersions, group management, ...) go
/// to any known broker, falling back to the bootstrap node.
public final class LeaderRouter {

    private static final int PRODUCE = 0;
    private static final int FETCH = 1;
    private static final int LIST_OFFSETS = 2;
    private static final int OFFSET_COMMIT = 8;
    private static final int OFFSET_FETCH = 9;
    private static final String CONSUMER_OFFSETS_TOPIC = "__consumer_offsets";

    private final MetadataCache cache;

    public LeaderRouter(MetadataCache cache) {
        this.cache = cache;
    }

    public Route route(Request request) {
        Object body = request.body();
        if (body == null) {
            return anyBroker();
        }
        int leader = switch (request.apiKey()) {
            case PRODUCE -> leaderOfFirstProduceTopic((ProduceRequestData) body);
            case FETCH -> leaderOfFirstFetchTopic((FetchRequestData) body);
            case LIST_OFFSETS -> leaderOfFirstListOffsetsTopic((ListOffsetsRequestData) body);
            case OFFSET_COMMIT -> leaderOfFirstOffsetCommitTopic((OffsetCommitRequestData) body);
            case OFFSET_FETCH -> leaderOfFirstOffsetFetchTopic((OffsetFetchRequestData) body);
            default -> anyBrokerId();
        };
        return leader >= 0 ? Route.to(leader) : anyBroker();
    }

    private int leaderOfFirstProduceTopic(ProduceRequestData data) {
        for (ProduceRequestData.TopicProduceData topic : data.topicData()) {
            for (ProduceRequestData.PartitionProduceData partition : topic.partitionData()) {
                int leader = cache.leaderFor(topic.name(), partition.index());
                if (leader >= 0) {
                    return leader;
                }
            }
        }
        return -1;
    }

    private int leaderOfFirstFetchTopic(FetchRequestData data) {
        for (FetchRequestData.FetchTopic topic : data.topics()) {
            for (FetchRequestData.FetchPartition partition : topic.partitions()) {
                int leader = cache.leaderFor(topic.topic(), partition.partition());
                if (leader >= 0) {
                    return leader;
                }
            }
        }
        return -1;
    }

    private int leaderOfFirstListOffsetsTopic(ListOffsetsRequestData data) {
        for (ListOffsetsRequestData.ListOffsetsTopic topic : data.topics()) {
            for (ListOffsetsRequestData.ListOffsetsPartition partition : topic.partitions()) {
                int leader = cache.leaderFor(topic.name(), partition.partitionIndex());
                if (leader >= 0) {
                    return leader;
                }
            }
        }
        return -1;
    }

    private int leaderOfFirstOffsetCommitTopic(OffsetCommitRequestData data) {
        int coordinator = coordinatorForGroup(data.groupId());
        if (coordinator >= 0) {
            return coordinator;
        }
        for (OffsetCommitRequestData.OffsetCommitRequestTopic topic : data.topics()) {
            int leader = cache.leaderForAny(topic.name());
            if (leader >= 0) {
                return leader;
            }
        }
        return -1;
    }

    private int leaderOfFirstOffsetFetchTopic(OffsetFetchRequestData data) {
        int coordinator = coordinatorForGroup(data.groupId());
        if (coordinator >= 0) {
            return coordinator;
        }
        for (OffsetFetchRequestData.OffsetFetchRequestTopic topic : data.topics()) {
            int leader = cache.leaderForAny(topic.name());
            if (leader >= 0) {
                return leader;
            }
        }
        return -1;
    }

    private int coordinatorForGroup(String groupId) {
        if (groupId == null || groupId.isEmpty()) {
            return -1;
        }
        int partitionCount = cache.partitionCount(CONSUMER_OFFSETS_TOPIC);
        if (partitionCount <= 0) {
            return -1;
        }
        int partition = Utils.toPositive(Utils.murmur2(groupId.getBytes())) % partitionCount;
        return cache.leaderFor(CONSUMER_OFFSETS_TOPIC, partition);
    }

    private int anyBrokerId() {
        return cache.brokers().stream()
                .mapToInt(BrokerNode::id)
                .min()
                .orElse(-1);
    }

    private Route anyBroker() {
        int brokerId = anyBrokerId();
        return brokerId >= 0 ? Route.to(brokerId) : Route.UNROUTED;
    }
}
