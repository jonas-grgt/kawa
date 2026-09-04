package io.jonasg.kawa.config;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public record AuthConfig(
        Set<String> mechanisms,
        Map<String, UserConfig> users,
        BrokerAuthConfig brokerAuth
) {

    public AuthConfig {
        mechanisms = mechanisms == null ? Set.of() : Set.copyOf(mechanisms);
        users = users == null ? Map.of() : resolveUserMechanisms(users, mechanisms);
        validateUserMechanisms(users, mechanisms);
    }

    private static Map<String, UserConfig> resolveUserMechanisms(
            Map<String, UserConfig> users,
            Set<String> mechanisms
    ) {
        // Deterministic default: pick the alphabetically-first configured mechanism so a
        // user without an explicit mechanism resolves consistently regardless of Set order.
        String globalMechanism = mechanisms.stream().sorted().findFirst().orElse(null);
        return users.entrySet().stream().collect(Collectors.toUnmodifiableMap(
                Map.Entry::getKey,
                entry -> resolveUser(entry.getKey(), entry.getValue(), globalMechanism)));
    }

    private static UserConfig resolveUser(
            String username,
            UserConfig user,
            String globalMechanism
    ) {
        if (user == null) {
            throw new IllegalArgumentException("user config for '" + username + "' must not be null");
        }
        String mechanism = user.mechanism();
        if (mechanism == null || mechanism.isBlank()) {
            if (globalMechanism == null) {
                throw new IllegalArgumentException("user '" + username
                        + "' has no mechanism and no global mechanism is configured");
            }
            return new UserConfig(globalMechanism, user.password());
        }
        return user;
    }

    private static void validateUserMechanisms(
            Map<String, UserConfig> users,
            Set<String> mechanisms
    ) {
        for (var entry : users.entrySet()) {
            String username = entry.getKey();
            String mechanism = entry.getValue().mechanism();
            if (!mechanisms.contains(mechanism)) {
                throw new IllegalArgumentException(
                        "User '" + username + "' uses mechanism '" + mechanism
                                + "' which is not in the configured mechanisms list " + mechanisms);
            }
        }
    }
}
