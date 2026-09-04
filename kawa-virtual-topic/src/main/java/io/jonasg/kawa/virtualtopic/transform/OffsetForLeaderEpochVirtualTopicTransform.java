package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;

import java.util.Objects;

public final class OffsetForLeaderEpochVirtualTopicTransform
        implements VirtualTopicTransform<OffsetForLeaderEpochRequestData, OffsetForLeaderEpochResponseData> {

    private static final short OFFSET_FOR_LEADER_EPOCH = 23;

    private final VirtualTopicManager virtualTopics;

    public OffsetForLeaderEpochVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return OFFSET_FOR_LEADER_EPOCH;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            OffsetForLeaderEpochRequestData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (OffsetForLeaderEpochRequestData.OffsetForLeaderTopic topic : data.topics()) {
            String logical = topic.topic();
            String physical = virtualTopics.toPhysical(logical);
            if (!Objects.equals(physical, logical)) {
                state.record(physical, logical);
                topic.setTopic(physical);
            }
        }
    }

    @Override
    public void onResponse(
            GatewayContext context,
            OffsetForLeaderEpochResponseData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult topic : data.topics()) {
            String logical = state.logicalFor(topic.topic());
            if (logical != null) {
                topic.setTopic(logical);
            }
        }
    }
}
