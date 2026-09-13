package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AuthConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.UserConfig;

import java.util.Comparator;
import java.util.Map;

/// Serves `/config/auth/users`: lists the users, upserts one entry via
/// `PUT /config/auth/users/{name}` and removes it via `DELETE /config/auth/users/{name}`.
/// Each write persists a full [GatewayConfig] snapshot through the [GatewayConfigRepository].
/// Adding a user auto-expands the advertised SASL mechanisms to include the user's mechanism,
/// so the first user can be added to an empty config.
public final class AuthUsersConfigHandler extends ConfigSectionHandler<UserConfig> {

    public AuthUsersConfigHandler(GatewayConfigRepository repository) {
        super(repository, UserConfig.class, "user");
    }

    @Override
    protected Map<String, UserConfig> entries(GatewayConfig config) {
        return config.auth().users();
    }

    @Override
    protected Object listView(GatewayConfig config) {
        return entries(config).entrySet().stream()
                .map(entry -> new UserView(entry.getKey(), entry.getValue().mechanism()))
                .sorted(Comparator.comparing(UserView::username))
                .toList();
    }

    @Override
    protected GatewayConfig upsert(GatewayConfig config, String name, UserConfig value) {
        AuthConfig auth = config.auth().withUser(name, value);
        return config.updateAuth(auth);
    }

    @Override
    protected GatewayConfig remove(GatewayConfig config, String name) {
        return config.updateAuth(config.auth().withoutUser(name));
    }

    /// `PATCH` updates one or both credential fields of an existing user; a field left out of
    /// the body keeps its stored value. The password stays write-only — it is never returned.
    @Override
    public Router.Response<?> handle(Router.Request request) {
        if (!"PATCH".equals(request.method())) {
            return super.handle(request);
        }
        GatewayConfig base = repository.getActiveConfigOrEmpty();
        String name = request.pathParams().get("name");
        UserConfig current = entries(base).get(name);
        if (current == null) {
            return Router.Response.notFound("user '" + name + "' not found");
        }
        UserConfigPatch patch;
        try {
            patch = mapper.readValue(request.body(), UserConfigPatch.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid user body: " + e.getMessage());
        }
        if (patch.mechanism() == null && patch.password() == null) {
            return Router.Response.badRequest("no fields to patch");
        }
        String mechanism = patch.mechanism() != null ? patch.mechanism() : current.mechanism();
        String password = patch.password() != null ? patch.password() : current.password();
        try {
            repository.update(config -> upsert(config, name, new UserConfig(mechanism, password)));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
        return Router.Response.ok(new UserView(name, mechanism));
    }
}