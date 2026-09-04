package io.jonasg.kawa.core;

/// Generic request/response transform contract for a single API, identified by its opaque
/// api key. Per-request state is carried on [GatewayContext]; concrete transform
/// families access it in their own type.
///
/// @param <Req>  the decoded request body type
/// @param <Resp> the decoded response body type
public interface ApiTransform<Req, Resp> {

    short apiKey();

    void onRequest(
            GatewayContext context,
            Req body
    );

    void onResponse(
            GatewayContext context,
            Resp body
    );
}
