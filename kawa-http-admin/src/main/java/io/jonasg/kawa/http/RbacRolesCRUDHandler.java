package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.RoleConfig;

import java.util.Comparator;
import java.util.Map;

final class RbacRolesCRUDHandler extends BaseCRUDHandler<RoleConfig> {

    RbacRolesCRUDHandler(GatewayConfigRepository repository) {
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
        return config.updateRbac(config.rbac().upsertRole(name, value));
    }

    @Override
    protected GatewayConfig remove(GatewayConfig config, String name) {
        return config.updateRbac(config.rbac().removeRole(name));
    }
}
