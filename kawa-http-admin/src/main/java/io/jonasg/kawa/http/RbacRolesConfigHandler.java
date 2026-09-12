package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.RoleConfig;

import java.util.Comparator;
import java.util.Map;

/// Serves `/config/rbac/roles`: lists the roles, upserts one entry via
/// `PUT /config/rbac/roles/{name}` and removes it via `DELETE /config/rbac/roles/{name}`.
/// Each write persists a full [GatewayConfig] snapshot through the [GatewayConfigRepository].
public final class RbacRolesConfigHandler extends ConfigSectionHandler<RoleConfig> {

    public RbacRolesConfigHandler(GatewayConfigRepository repository) {
        super(repository, RoleConfig.class, "role");
    }

    @Override
    protected Map<String, RoleConfig> entries(GatewayConfig config) {
        return config.rbac().roles();
    }

    @Override
    protected Object listView(GatewayConfig config) {
        return entries(config).entrySet().stream()
                .map(entry -> new RoleView(entry.getKey(), entry.getValue().acls()))
                .sorted(Comparator.comparing(RoleView::name))
                .toList();
    }

    @Override
    protected GatewayConfig upsert(GatewayConfig config, String name, RoleConfig value) {
        return config.updateRbac(config.rbac().withRole(name, value));
    }

    @Override
    protected GatewayConfig remove(GatewayConfig config, String name) {
        return config.updateRbac(config.rbac().withoutRole(name));
    }
}