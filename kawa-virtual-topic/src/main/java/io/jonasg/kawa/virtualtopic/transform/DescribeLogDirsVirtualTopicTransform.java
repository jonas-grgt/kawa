package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.VirtualTopicManager;
import io.jonasg.kawa.virtualtopic.VirtualTopicState;
import org.apache.kafka.common.message.DescribeLogDirsRequestData;
import org.apache.kafka.common.message.DescribeLogDirsResponseData;

import java.util.Objects;

public final class DescribeLogDirsVirtualTopicTransform
        implements VirtualTopicTransform<DescribeLogDirsRequestData, DescribeLogDirsResponseData> {

    private static final short DESCRIBE_LOG_DIRS = 35;

    private final VirtualTopicManager virtualTopics;

    public DescribeLogDirsVirtualTopicTransform(VirtualTopicManager virtualTopics) {
        this.virtualTopics = virtualTopics;
    }

    @Override
    public short apiKey() {
        return DESCRIBE_LOG_DIRS;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            DescribeLogDirsRequestData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (DescribeLogDirsRequestData.DescribableLogDirTopic topic : data.topics()) {
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
            DescribeLogDirsResponseData data
    ) {
        VirtualTopicState state = VirtualTopicState.from(context);
        for (DescribeLogDirsResponseData.DescribeLogDirsResult result : data.results()) {
            for (DescribeLogDirsResponseData.DescribeLogDirsTopic topic : result.topics()) {
                String logical = state.logicalFor(topic.name());
                if (logical != null) {
                    topic.setName(logical);
                }
            }
        }
    }
}
