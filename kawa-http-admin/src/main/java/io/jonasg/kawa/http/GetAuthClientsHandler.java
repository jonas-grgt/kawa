package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;

/// Serves `GET /auth/clients`.
public final class GetAuthClientsHandler implements Router.Handler {

    private final AuthClientsCRUDHandler config;

    public GetAuthClientsHandler(GatewayConfigRepository repository) {
        this.config = new AuthClientsCRUDHandler(repository);
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return config.get(request);
    }
}
