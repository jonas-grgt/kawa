package io.jonasg.kawa.core;

/// A gateway interceptor. Interceptors observe and rewrite requests in the client-to-broker
/// direction ([#onRequest]) and responses in the broker-to-client direction
/// ([#onResponse]).
///
/// The core invokes interceptors in registration order and knows nothing about concrete
/// implementations (virtual topics, auth, metrics, etc.).
public interface Interceptor {

    default boolean appliesToRequest(Request request) {
        return true;
    }

    default boolean appliesToResponse(Response response) {
        return true;
    }

    default void onRequest(
            GatewayContext context,
            Request request
    ) {
        // no-op
    }

    default void onResponse(
            GatewayContext context,
            Response response
    ) {
        // no-op
    }
}
