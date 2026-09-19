package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;

/// Serves `DELETE /rbac/roles/{name}`.
public final class DeleteRbacRoleHandler implements Router.Handler {

    private final RbacRolesCRUDHandler crudHandler;

    public DeleteRbacRoleHandler(GatewayConfigRepository repository) {
        this.crudHandler = new RbacRolesCRUDHandler(repository);
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return crudHandler.delete(request);
    }
}
