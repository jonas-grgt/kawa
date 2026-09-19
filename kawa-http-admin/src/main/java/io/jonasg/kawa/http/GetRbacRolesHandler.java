package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;

/// Serves `GET /rbac/roles`.
public final class GetRbacRolesHandler implements Router.Handler {

    private final RbacRolesCRUDHandler crudHandler;

    public GetRbacRolesHandler(GatewayConfigRepository repository) {
        this.crudHandler = new RbacRolesCRUDHandler(repository);
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return crudHandler.get(request);
    }
}
