package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.governance.GovernancePolicy;
import tools.jackson.databind.json.JsonMapper;

/// Serves `/config/governance`: lists the whole governance section (rules + exemptions) via
/// `GET` and replaces it via `PUT`. Rule expressions are validated with the [GovernancePolicy]
/// before the new snapshot is persisted, so a bad expression is rejected by the admin API
/// instead of failing the config-topic consumer later.
public final class GovernanceConfigHandler implements Router.Handler {

    private final GatewayConfigRepository repository;
    private final ConsistencyAwareUpdater updater;
    private final GovernancePolicy governance;
    private final JsonMapper mapper = JsonMapper.builder().build();

    public GovernanceConfigHandler(GatewayConfigRepository repository, GovernancePolicy governance) {
        this.repository = repository;
        this.updater = new ConsistencyAwareUpdater(repository);
        this.governance = governance;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        GatewayConfig base = repository.getActiveConfigOrEmpty();
        return switch (request.method()) {
            case "GET" -> Router.Response.ok(base.governance());
            case "PUT" -> {
                GovernanceConfig value;
                try {
                    value = mapper.readValue(request.body(), GovernanceConfig.class);
                } catch (Exception e) {
                    yield Router.Response.badRequest("invalid governance body: " + e.getMessage());
                }
                for (var entry : value.topicRules().entrySet()) {
                    var error = governance.validationError(entry.getValue().expression());
                    if (error.isPresent()) {
                        yield Router.Response.badRequest(
                                "invalid governance rule '" + entry.getKey() + "': " + error.get());
                    }
                }
                try {
                    updater.update(request, config -> config.updateGovernance(value));
                } catch (IllegalArgumentException e) {
                    yield Router.Response.badRequest(e.getMessage());
                }
                yield Router.Response.ok(value);
            }
            default -> Router.Response.badRequest("unsupported method " + request.method());
        };
    }
}
