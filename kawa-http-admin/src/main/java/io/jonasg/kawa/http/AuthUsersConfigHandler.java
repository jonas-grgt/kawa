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
                .map(entry -> new UserView(entry.getKey(), entry.getValue().mechanism(), entry.getValue().password()))
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
}