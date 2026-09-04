package io.jonasg.kawa.http;

import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.VirtualTopicFilterConfig;
import io.jonasg.kawa.core.VirtualTopicManager;
import io.jonasg.kawa.core.cluster.MetadataCache;

import java.util.ArrayList;
import java.util.List;

/// Projects the logical and physical topics served by `GET /topics` from the [VirtualTopicManager]
/// (logical config) and the [MetadataCache] (live physical topology). Plain handler with no Netty
/// imports; the [HttpRouterHandler] dispatcher serializes the result and writes the response.
public final class TopicsHandler implements Router.Handler<TopicView> {

    private final VirtualTopicManager virtualTopics;
    private final MetadataCache cache;

    public TopicsHandler(VirtualTopicManager virtualTopics, MetadataCache cache) {
        this.virtualTopics = virtualTopics;
        this.cache = cache;
    }

    @Override
    public List<TopicView> handle() {
        List<TopicView> views = new ArrayList<>();
        for (String physical : cache.topics()) {
            boolean virtualized = virtualTopics.hasVirtualTopic(physical);
            views.add(new TopicView(
                    virtualized ? virtualTopics.toLogical(physical) : null,
                    physical,
                    cache.partitionCount(physical),
                    virtualized ? describeFilter(virtualTopics.filterFor(physical).orElse(null)) : null,
                    virtualized && virtualTopics.exposesPhysicalTopic(physical)));
        }
        return views;
    }

    private static String describeFilter(VirtualTopicFilterConfig filter) {
		return switch (filter) {
		    case null -> null;
		    case HeaderEqualsFilterConfig header -> "headerEquals(" + header.header() + "=" + header.value() + ")";
		    case CelFilterConfig cel -> "cel(" + cel.expression() + ")";
		};
	}
}
