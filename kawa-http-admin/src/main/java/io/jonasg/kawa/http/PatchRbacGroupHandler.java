package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;

/// Serves `PATCH /rbac/groups/{name}`.
public final class PatchRbacGroupHandler implements Router.Handler {

    private final RbacGroupsCRUDHandler config;

    public PatchRbacGroupHandler(GatewayConfigRepository repository) {
        this.config = new RbacGroupsCRUDHandler(repository);
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return config.patch(request);
    }
}
