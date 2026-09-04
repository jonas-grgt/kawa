package io.jonasg.kawa.http;

import io.netty.handler.codec.http.HttpMethod;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/// A tiny, data-driven HTTP route table. Routes are keyed by `(method, path)` so that a path
/// registered for one method but hit with another can be distinguished from a wholly unknown path.
public final class Router {

    /// A route handler. Implementations are plain (no Netty imports); the [HttpRouterHandler]
    /// dispatcher is responsible for serializing the returned topics and writing the HTTP response.
    @FunctionalInterface
    public interface Handler<T> {
        List<T> handle();
    }

    private record RouteKey(HttpMethod method, String path) {}

    private final Map<RouteKey, Handler<?>> routes = new HashMap<>();

    /// Registers a handler for `GET path` and returns `this` for chaining.
    public Router get(String path, Handler<?> handler) {
        routes.put(new RouteKey(HttpMethod.GET, path), handler);
        return this;
    }

    /// Returns the handler registered for `(method, path)`, if any.
    public Optional<Handler<?>> find(HttpMethod method, String path) {
        return Optional.ofNullable(routes.get(new RouteKey(method, path)));
    }

    /// Returns whether any route is registered for `path` regardless of method.
    public boolean hasPath(String path) {
        return routes.keySet().stream().anyMatch(key -> key.path().equals(path));
    }
}
