package io.jonasg.kawa.config;

/// Filter that evaluates a CEL (Common Expression Language) expression against each record.
/// The expression has access to `key`, `value`, `headers`, and `timestamp` bindings.
///
/// @param expression the CEL expression to evaluate — must return a boolean
public record CelFilterConfig(String expression) implements VirtualTopicFilterConfig {
}
