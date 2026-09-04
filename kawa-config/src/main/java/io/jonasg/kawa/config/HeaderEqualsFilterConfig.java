package io.jonasg.kawa.config;

/// Filter that keeps only records whose `header` record header equals `value`.
///
/// @param header header key to compare
/// @param value expected header value
public record HeaderEqualsFilterConfig(
        String header,
        String value
) implements VirtualTopicFilterConfig {
}
