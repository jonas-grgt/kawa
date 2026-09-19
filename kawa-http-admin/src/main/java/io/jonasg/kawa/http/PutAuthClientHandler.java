package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;

/// Serves `PUT /auth/clients/{name}`.
public final class PutAuthClientHandler implements Router.Handler {

    private final AuthClientsCRUDHandler config;

    public PutAuthClientHandler(GatewayConfigRepository repository) {
        this.config = new AuthClientsCRUDHandler(repository);
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return config.put(request);
    }
}
