package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.GroupConfig;

import java.util.Map;

/// Serves `/config/rbac/groups`: lists the group map, upserts one entry via
/// `PUT /config/rbac/groups/{name}` and removes it via `DELETE /config/rbac/groups/{name}`.
/// Each write persists a full [GatewayConfig] snapshot through the [GatewayConfigRepository].
public final class RbacGroupsConfigHandler extends ConfigSectionHandler<GroupConfig> {

    public RbacGroupsConfigHandler(GatewayConfigRepository repository) {
        super(repository, GroupConfig.class, "group");
    }

    @Override
    protected Map<String, GroupConfig> entries(GatewayConfig config) {
        return config.rbac().groups();
    }

    @Override
    protected GatewayConfig upsert(GatewayConfig config, String name, GroupConfig value) {
        return config.updateRbac(config.rbac().withGroup(name, value));
    }

    @Override
    protected GatewayConfig remove(GatewayConfig config, String name) {
        return config.updateRbac(config.rbac().withoutGroup(name));
    }
}