package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.governance.GovernancePolicy;
import tools.jackson.databind.json.JsonMapper;

/// Serves `PUT /governance`.
public final class PutGovernanceHandler implements Router.Handler {

    private final GatewayConfigRepository repository;
    private final ConsistencyAwareUpdater updater;
    private final GovernancePolicy governance;
    private final JsonMapper mapper = JsonMapper.builder().build();

    public PutGovernanceHandler(GatewayConfigRepository repository, GovernancePolicy governance) {
        this.repository = repository;
        this.updater = new ConsistencyAwareUpdater(repository);
        this.governance = governance;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        GovernanceConfig value;
        try {
            value = mapper.readValue(request.body(), GovernanceConfig.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid governance body: " + e.getMessage());
        }
        for (var entry : value.topicRules().entrySet()) {
            var error = governance.validationError(entry.getValue().expression());
            if (error.isPresent()) {
                return Router.Response.badRequest(
                        "invalid governance rule '" + entry.getKey() + "': " + error.get());
            }
        }
        try {
            updater.update(request, config -> config.updateGovernance(value));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
        return Router.Response.ok(value);
    }
}
