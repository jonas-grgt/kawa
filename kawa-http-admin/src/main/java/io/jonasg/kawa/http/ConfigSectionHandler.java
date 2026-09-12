package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

/// Base for the per-section config handlers (`/config/...`). Each subclass maps one dynamic
/// config section (virtual topics, RBAC roles/groups, auth users) onto the [GatewayConfig]
/// snapshot: [entries] reads the section, [upsert] and [remove] produce a new snapshot with
/// one entry changed. Plain handler with no Netty imports; the [HttpRouterHandler] dispatcher
/// serializes the result and writes the response.
///
/// `GET` lists the section, `PUT /{name}` upserts one entry (persisting the new snapshot via
/// the [GatewayConfigRepository] and returning the stored entry), `DELETE /{name}` removes it
/// (404 when it does not exist). The section starts empty when no snapshot has been applied yet.
abstract class ConfigSectionHandler<T> implements Router.Handler {

    private final GatewayConfigRepository repository;
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final Class<T> valueType;
    private final String sectionName;

    ConfigSectionHandler(GatewayConfigRepository repository, Class<T> valueType, String sectionName) {
        this.repository = repository;
        this.valueType = valueType;
        this.sectionName = sectionName;
    }

    /// The section's entries from a snapshot.
    protected abstract Map<String, T> entries(GatewayConfig config);

    /// A new snapshot with the given entry added or replaced.
    protected abstract GatewayConfig upsert(GatewayConfig config, String name, T value);

    /// A new snapshot with the given entry removed.
    protected abstract GatewayConfig remove(GatewayConfig config, String name);

    @Override
    public Router.Response<?> handle(Router.Request request) {
        GatewayConfig base = repository.getActiveConfigOrEmpty();
        String name = request.pathParams().get("name");
        return switch (request.method()) {
            case "GET" -> Router.Response.ok(entries(base));
            case "PUT" -> {
                T value;
                try {
                    value = mapper.readValue(request.body(), valueType);
                } catch (Exception e) {
                    yield Router.Response.badRequest("invalid " + sectionName + " body: " + e.getMessage());
                }
                try {
                    repository.update(config -> upsert(config, name, value));
                } catch (IllegalArgumentException e) {
                    yield Router.Response.badRequest(e.getMessage());
                }
                yield Router.Response.ok(value);
            }
            case "DELETE" -> {
                if (!entries(base).containsKey(name)) {
                    yield Router.Response.notFound(sectionName + " '" + name + "' not found");
                }
                repository.update(config -> remove(config, name));
                yield Router.Response.noContent();
            }
            default -> Router.Response.badRequest("unsupported method " + request.method());
        };
    }
}