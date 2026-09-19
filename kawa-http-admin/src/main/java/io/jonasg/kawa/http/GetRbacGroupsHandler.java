package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;

/// Serves `GET /rbac/groups`.
public final class GetRbacGroupsHandler implements Router.Handler {

    private final RbacGroupsCRUDHandler config;

    public GetRbacGroupsHandler(GatewayConfigRepository repository) {
        this.config = new RbacGroupsCRUDHandler(repository);
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return config.get(request);
    }
}
