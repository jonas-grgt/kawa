package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;

/// Serves `PATCH /auth/clients/{name}`.
public final class PatchAuthClientHandler implements Router.Handler {

    private final AuthClientsCRUDHandler config;

    public PatchAuthClientHandler(GatewayConfigRepository repository) {
        this.config = new AuthClientsCRUDHandler(repository);
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return config.patch(request);
    }
}
