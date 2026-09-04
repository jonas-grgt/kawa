package io.jonasg.kawa.core;

/// A decoded request flowing through the gateway. Transport-agnostic: the body is
/// an opaque decoded object supplied by the protocol layer.
public interface Request {

    int apiKey();

    String apiName();

    short apiVersion();

    int correlationId();

    /// Client id from the request header, or `null`.
    String clientId();

    /// Decoded body, or `null` for requests that are passed through without inspection.
    Object body();
}
