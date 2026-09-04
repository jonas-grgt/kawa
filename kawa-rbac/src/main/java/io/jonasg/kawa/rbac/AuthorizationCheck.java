package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;

/// One apiKey's RBAC enforcement, dispatched by [AuthorizationCheckRegistry].
///
/// This does not extend [io.jonasg.kawa.core.ApiTransform]. That contract's `onRequest` only
/// receives the decoded body, which works for virtual-topic rewriting but not here: building a
/// denial response needs the wire apiVersion (to construct a
/// [io.jonasg.kawa.core.ShortCircuitResult]), and unlike `VirtualTopicState.apiVersion()` there
/// is no existing per-request state to stash it in. Rather than inventing one, `onRequest` takes
/// apiVersion as a plain parameter.
public interface AuthorizationCheck<Req, Resp> {

    short apiKey();

    /// Called once per request for this apiKey. Implementations read
    /// [GatewayContext#principal] themselves (it may be `null`, meaning the client skipped
    /// SASL) and are responsible for denying rather than passing through in that case, exactly
    /// as today's whole-request gates and `handleProduce` already do.
    void onRequest(GatewayContext context, short apiVersion, Req body);

    /// Called once per response for this apiKey. No-op by default; only Produce needs it.
    default void onResponse(GatewayContext context, Resp body) {
    }
}
