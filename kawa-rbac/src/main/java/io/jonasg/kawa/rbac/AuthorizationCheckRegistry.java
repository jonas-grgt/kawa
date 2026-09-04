package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/// Registry for RBAC authorization checks. Owns the single unchecked cast per direction
/// (`Object → Req` / `Object → Resp`) so that check implementations stay cast-free.
public final class AuthorizationCheckRegistry {

    private final Map<Short, AuthorizationCheck<?, ?>> checks;

    public AuthorizationCheckRegistry(Collection<? extends AuthorizationCheck<?, ?>> checks) {
        Map<Short, AuthorizationCheck<?, ?>> byKey = new HashMap<>();
        for (AuthorizationCheck<?, ?> check : checks) {
            if (byKey.put(check.apiKey(), check) != null) {
                throw new IllegalArgumentException("Duplicate authorization check for api key " + check.apiKey());
            }
        }
        this.checks = Map.copyOf(byKey);
    }

    @SuppressWarnings("unchecked")
    public void onRequest(GatewayContext context, short apiKey, short apiVersion, Object body) {
        AuthorizationCheck<Object, Object> check = (AuthorizationCheck<Object, Object>) checks.get(apiKey);
        if (check != null) {
            check.onRequest(context, apiVersion, body);
        }
    }

    @SuppressWarnings("unchecked")
    public void onResponse(GatewayContext context, short apiKey, Object body) {
        if (body == null) {
            return;
        }
        AuthorizationCheck<Object, Object> check = (AuthorizationCheck<Object, Object>) checks.get(apiKey);
        if (check != null) {
            check.onResponse(context, body);
        }
    }

    public boolean hasApiKey(short apiKey) {
        return checks.containsKey(apiKey);
    }

    public Set<Short> apiKeys() {
        return Set.copyOf(checks.keySet());
    }
}
