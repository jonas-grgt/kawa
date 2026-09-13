package io.jonasg.kawa.config;

/// Filter that keeps only records with a `header` record header whose value contains `value`.
///
/// @param header header key to compare
/// @param value substring the header value must contain
public record HeaderContainsFilterConfig(
        String header,
        String value
) implements VirtualTopicFilterConfig {
}