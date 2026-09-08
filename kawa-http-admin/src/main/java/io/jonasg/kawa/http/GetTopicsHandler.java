package io.jonasg.kawa.http;

import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.VirtualTopicFilterConfig;
import io.jonasg.kawa.core.VirtualTopicManager;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.cluster.TopicMetadata;

import java.util.ArrayList;
import java.util.List;

/// Projects the virtual and physical topics served by `GET /topics` from the [VirtualTopicManager]
/// (virtual config) and the [MetadataCache] (live physical topology). Plain handler with no Netty
/// imports; the [HttpRouterHandler] dispatcher serializes the result and writes the response.
public final class GetTopicsHandler implements Router.Handler {

    private final VirtualTopicManager virtualTopics;
    private final MetadataCache cache;

    public GetTopicsHandler(VirtualTopicManager virtualTopics, MetadataCache cache) {
        this.virtualTopics = virtualTopics;
        this.cache = cache;
    }

    @Override
    public Router.Response<List<TopicView>> handle(Router.Request request) {
        List<TopicView> views = new ArrayList<>();
        for (TopicMetadata tm : cache.topics()) {
            boolean virtualized = virtualTopics.hasVirtualTopic(tm.name());
            if (virtualized) {
                views.add(new TopicView(
                        "virtual",
                        virtualTopics.toVirtual(tm.name()),
                        cache.partitionCount(tm.name()),
                        cache.replicationFactor(tm.name()),
                        toFilterView(virtualTopics.filterFor(tm.name()).orElse(null)),
                        tm.name()));
            }
            views.add(new TopicView(
                    "physical",
                    tm.name(),
                    cache.partitionCount(tm.name()),
                    cache.replicationFactor(tm.name()),
                    null,
                    null));
        }
        return Router.Response.ok(views);
    }

    private static TopicFilterView toFilterView(VirtualTopicFilterConfig filter) {
        return switch (filter) {
            case null -> null;
            case HeaderEqualsFilterConfig header ->
                    new TopicFilterView("header", header.header() + "=" + header.value());
            case CelFilterConfig cel ->
                    new TopicFilterView("cel", cel.expression());
        };
    }
}
