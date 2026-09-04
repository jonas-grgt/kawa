package io.jonasg.kawa.core;

/// Marks a request as answered locally by the gateway instead of being forwarded to a broker.
/// Carries the decoded Kafka response [body](#body) the transport should encode and send to the client.
///
/// @param apiKey the API this short-circuit belongs to
/// @param apiVersion the response version to encode
/// @param body decoded Kafka response body (POJO from the Kafka messages library)
public record ShortCircuitResult(short apiKey, short apiVersion, Object body) {
}
