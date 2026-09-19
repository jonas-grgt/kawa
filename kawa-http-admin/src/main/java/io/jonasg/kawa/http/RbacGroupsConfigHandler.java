package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.GroupConfig;

import java.util.Comparator;
import java.util.Map;

/// Serves `/config/rbac/groups`: lists the groups, upserts one entry via
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
    protected Object listView(GatewayConfig config) {
        return entries(config).entrySet().stream()
                .map(entry -> new GroupView(entry.getKey(), entry.getValue().clients(), entry.getValue().roles()))
                .sorted(Comparator.comparing(GroupView::name))
                .toList();
    }

    @Override
    protected Router.Response<?> validateRemove(GatewayConfig config, String name) {
        GroupConfig group = entries(config).get(name);
        if (group.clients().isEmpty()) {
            return null;
        }
        return Router.Response.conflict("group '" + name + "' still has clients " + group.clients());
    }

    @Override
    protected GatewayConfig upsert(GatewayConfig config, String name, GroupConfig value) {
        return config.updateRbac(config.rbac().withGroup(name, value));
    }

    @Override
    protected GatewayConfig remove(GatewayConfig config, String name) {
        return config.updateRbac(config.rbac().removeGroup(name));
    }

    /// `PATCH` renames the group: the body carries the new name, and the group's clients and
    /// roles move with it. The old name must exist and the new name must be free.
    @Override
    public Router.Response<?> handle(Router.Request request) {
        if (!"PATCH".equals(request.method())) {
            return super.handle(request);
        }
        GatewayConfig base = repository.getActiveConfigOrEmpty();
        String name = request.pathParams().get("name");
        GroupConfig current = entries(base).get(name);
        if (current == null) {
            return Router.Response.notFound("group '" + name + "' not found");
        }
        GroupConfigPatch patch;
        try {
            patch = mapper.readValue(request.body(), GroupConfigPatch.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid group body: " + e.getMessage());
        }
        String newName = patch.name();
        if (newName == null || newName.isBlank()) {
            return Router.Response.badRequest("no new name to rename to");
        }
        if (newName.equals(name)) {
            return Router.Response.badRequest("group is already named '" + name + "'");
        }
        if (entries(base).containsKey(newName)) {
            return Router.Response.conflict("group '" + newName + "' already exists");
        }
        try {
            updater.update(request, config -> config.updateRbac(config.rbac().renameGroup(name, newName)));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
        return Router.Response.ok(new GroupView(newName, current.clients(), current.roles()));
    }
}
