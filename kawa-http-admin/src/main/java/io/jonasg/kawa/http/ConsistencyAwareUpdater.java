package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;

import java.util.Objects;
import java.util.function.UnaryOperator;

final class ConsistencyAwareUpdater {

	private final GatewayConfigRepository repository;

	ConsistencyAwareUpdater(GatewayConfigRepository repository) {
		this.repository = repository;
	}

	void update(
			Router.Request request,
			UnaryOperator<GatewayConfig> mutation
	) {
		String consistency = request.queryParams().get("consistency");
		if (!Objects.equals(consistency, "applied") && consistency != null) {
			throw new IllegalArgumentException(
					"invalid consistency '" + consistency + "' (expected 'persisted')");
		}
		if ("applied".equals(consistency)) {
			repository.updateAndWaitUntilApplied(mutation);
			return;
		}
		repository.update(mutation);
	}
}
