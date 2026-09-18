package io.jonasg.kawa.http;

import io.jonasg.kawa.config.VirtualTopicFilterConfig;

/// A partial update for an existing virtual topic. Missing fields keep their current values;
/// an omitted or null `filter` clears the current filter.
public record VirtualTopicConfigPatch(
        String name,
        String topic,
        VirtualTopicFilterConfig filter,
        Boolean exposePhysicalTopic
) {
}
