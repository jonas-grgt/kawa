package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;

/// Serves `DELETE /auth/clients/{name}`.
public final class DeleteAuthClientHandler implements Router.Handler {

    private final AuthClientsCRUDHandler crudHandler;

    public DeleteAuthClientHandler(GatewayConfigRepository repository) {
        this.crudHandler = new AuthClientsCRUDHandler(repository);
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        return crudHandler.delete(request);
    }
}
