package io.jonasg.kawa.http;

/// A structured consume-filter entry in the admin `/topics` response,
/// mirroring the frontend `TopicFilter` type.
///
/// @param kind either `"cel"` or `"header"`
/// @param expression the CEL expression, or the `header=value` pair for header filters
public record TopicFilterView(String kind, String expression) {
}
