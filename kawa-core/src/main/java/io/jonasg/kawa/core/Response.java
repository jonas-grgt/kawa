package io.jonasg.kawa.core;

/// A response being returned to a client. Transport-agnostic: the body is an opaque
/// decoded object supplied by the protocol layer.
public interface Response {

    int apiKey();

    String apiName();

    short apiVersion();

    int correlationId();

    /// Decoded body, or `null` for responses that are passed through without inspection.
    Object body();
}
