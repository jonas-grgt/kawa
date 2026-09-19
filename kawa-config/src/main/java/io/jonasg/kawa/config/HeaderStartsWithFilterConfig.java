package io.jonasg.kawa.config;

/// Filter that keeps only records with a `header` record header whose value starts with `value`.
///
/// @param header header key to compare
/// @param value  prefix the header value must start with
public record HeaderStartsWithFilterConfig(
        String header,
        String value
) implements VirtualTopicFilterConfig {
}
