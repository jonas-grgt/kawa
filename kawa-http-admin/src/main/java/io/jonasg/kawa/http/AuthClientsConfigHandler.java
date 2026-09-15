package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AuthConfig;
import io.jonasg.kawa.config.ClientConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/// Serves `/config/auth/clients`: lists the clients, upserts one entry via
/// `PUT /config/auth/clients/{name}` and removes it via `DELETE /config/auth/clients/{name}`.
/// Each write persists a full [GatewayConfig] snapshot through the [GatewayConfigRepository].
/// Adding a client auto-expands the advertised SASL mechanisms to include the client's
/// mechanism, so the first client can be added to an empty config.
public final class AuthClientsConfigHandler extends ConfigSectionHandler<ClientConfig> {

    public AuthClientsConfigHandler(GatewayConfigRepository repository) {
        super(repository, ClientConfig.class, "client");
    }

    @Override
    protected Map<String, ClientConfig> entries(GatewayConfig config) {
        return config.auth().clients();
    }

    @Override
    protected Object listView(GatewayConfig config) {
        return entries(config).entrySet().stream()
                .map(entry -> new ClientView(entry.getKey(), entry.getValue().mechanism()))
                .sorted(Comparator.comparing(ClientView::username))
                .toList();
    }

    @Override
    protected GatewayConfig upsert(GatewayConfig config, String name, ClientConfig value) {
        AuthConfig auth = config.auth().withClient(name, value);
        return config.updateAuth(auth);
    }

    @Override
    protected GatewayConfig remove(GatewayConfig config, String name) {
        return config.updateAuth(config.auth().withoutClient(name));
    }

    @Override
    protected Router.Response<?> validateRemove(GatewayConfig config, String name) {
        List<String> groups = config.rbac().groups().entrySet().stream()
                .filter(entry -> entry.getValue().clients().contains(name))
                .map(Map.Entry::getKey)
                .sorted()
                .toList();
        if (groups.isEmpty()) {
            return null;
        }
        return Router.Response.conflict("client '" + name + "' is still in groups " + groups);
    }

    /// `PATCH` updates one or both credential fields of an existing client; a field left out
    /// of the body keeps its stored value. The password stays write-only — it is never
    /// returned.
    @Override
    public Router.Response<?> handle(Router.Request request) {
        if (!"PATCH".equals(request.method())) {
            return super.handle(request);
        }
        GatewayConfig base = repository.getActiveConfigOrEmpty();
        String name = request.pathParams().get("name");
        ClientConfig current = entries(base).get(name);
        if (current == null) {
            return Router.Response.notFound("client '" + name + "' not found");
        }
        ClientConfigPatch patch;
        try {
            patch = mapper.readValue(request.body(), ClientConfigPatch.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid client body: " + e.getMessage());
        }
        if (patch.mechanism() == null && patch.password() == null) {
            return Router.Response.badRequest("no fields to patch");
        }
        String mechanism = patch.mechanism() != null ? patch.mechanism() : current.mechanism();
        String password = patch.password() != null ? patch.password() : current.password();
        try {
            updater.update(request, config -> upsert(config, name, new ClientConfig(mechanism, password)));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
        return Router.Response.ok(new ClientView(name, mechanism));
    }
}
