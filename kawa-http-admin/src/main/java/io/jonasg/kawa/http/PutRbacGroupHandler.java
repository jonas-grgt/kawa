package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;

/// Serves `PUT /rbac/groups/{name}`.
public final class PutRbacGroupHandler implements Router.Handler {

    private final RbacGroupsCRUDHandler config;

    public PutRbacGroupHandler(GatewayConfigRepository repository) {
        this.config = new RbacGroupsCRUDHandler(repository);
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return config.put(request);
    }
}
