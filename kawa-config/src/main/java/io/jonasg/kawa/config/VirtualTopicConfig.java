package io.jonasg.kawa.config;

/// Virtual topic configuration.
///
/// @param topic physical topic name
/// @param filter optional server-side consume filter configuration
/// @param exposePhysicalTopic when `true`, the physical topic is still listed alongside
///                            its logical name in Metadata responses instead of being hidden
///                            (renamed to the logical name in place) - hidden by default
public record VirtualTopicConfig(String topic, VirtualTopicFilterConfig filter, boolean exposePhysicalTopic) {

    public VirtualTopicConfig(String topic) {
        this(topic, null, false);
    }

    public VirtualTopicConfig(String topic, VirtualTopicFilterConfig filter) {
        this(topic, filter, false);
    }
}
