package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;

/// Serves `PUT /rbac/roles/{name}`.
public final class PutRbacRoleHandler implements Router.Handler {

    private final RbacRolesCRUDHandler crudHandler;

    public PutRbacRoleHandler(GatewayConfigRepository repository) {
        this.crudHandler = new RbacRolesCRUDHandler(repository);
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return crudHandler.put(request);
    }
}
