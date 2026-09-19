package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;

/// Serves `DELETE /rbac/groups/{name}`.
public final class DeleteRbacGroupHandler implements Router.Handler {

    private final RbacGroupsCRUDHandler config;

    public DeleteRbacGroupHandler(GatewayConfigRepository repository) {
        this.config = new RbacGroupsCRUDHandler(repository);
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return config.delete(request);
    }
}
