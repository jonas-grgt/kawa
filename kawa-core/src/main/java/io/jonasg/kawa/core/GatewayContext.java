package io.jonasg.kawa.core;

import java.util.HashMap;
import java.util.Map;

/// Per-request context passed to interceptors. Carries the (opaque) source connection,
/// timing information and the routing decision.
public final class GatewayContext {

    private final Object source;
    private final long receivedAtNanos;
    private Route route;
    private final Map<Class<?>, Object> states = new HashMap<>();
    private ShortCircuitResult shortCircuitResult;
    private String principal;

    public GatewayContext(
            Object source,
            long receivedAtNanos
    ) {
        this(source, receivedAtNanos, null);
    }

    public GatewayContext(
            Object source,
            long receivedAtNanos,
            String principal
    ) {
        this.source = source;
        this.receivedAtNanos = receivedAtNanos;
        this.principal = principal;
        this.route = Route.UNROUTED;
    }

    /// The transport connection the request arrived on (opaque to the core).
    public Object source() {
        return source;
    }

    public long receivedAtNanos() {
        return receivedAtNanos;
    }

    public Route route() {
        return route;
    }

    public void route(Route route) {
        this.route = route;
    }

    /// Interceptor scratch state for this request's lifetime, keyed by state type so that
    /// independent interceptor families can each hold their own per-request state without
    /// colliding. Opaque to the core.
    public <T> T state(Class<T> type) {
        return type.cast(states.get(type));
    }

    public void state(Class<?> type, Object value) {
        states.put(type, value);
    }

    /// Marks this request as answered locally by the gateway instead of being forwarded to a
    /// broker. Any interceptor may short-circuit a request; the pipeline stops invoking
    /// further interceptors once one has done so.
    public void shortCircuit(ShortCircuitResult result) {
        this.shortCircuitResult = result;
    }

    public boolean isShortCircuited() {
        return shortCircuitResult != null;
    }

    public ShortCircuitResult shortCircuitResult() {
        return shortCircuitResult;
    }

    /// The authenticated principal for the connection this request arrived on, or `null` if
    /// the connection has not authenticated yet.
    public String principal() {
        return principal;
    }
}
