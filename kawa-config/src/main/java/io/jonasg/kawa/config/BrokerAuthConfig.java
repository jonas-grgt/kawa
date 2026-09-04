package io.jonasg.kawa.config;

import java.util.function.Function;

public record BrokerAuthConfig(
        String mechanism,
        String username,
        String password
) {

    public BrokerAuthConfig {
        if (mechanism == null || mechanism.isBlank()) {
            throw new IllegalArgumentException("brokerAuth mechanism must not be null or blank");
        }
        if (username == null || username.isBlank()) {
            throw new IllegalArgumentException("brokerAuth username must not be null or blank");
        }
        if (password == null || password.isBlank()) {
            throw new IllegalArgumentException("brokerAuth password must not be null or blank");
        }
        password = UserConfig.resolveEnvVars(password, System::getenv);
    }

    static BrokerAuthConfig of(
            String mechanism,
            String username,
            String password,
            Function<String, String> envLookup
    ) {
        if (mechanism == null || mechanism.isBlank()) {
            throw new IllegalArgumentException("brokerAuth mechanism must not be null or blank");
        }
        if (username == null || username.isBlank()) {
            throw new IllegalArgumentException("brokerAuth username must not be null or blank");
        }
        if (password == null || password.isBlank()) {
            throw new IllegalArgumentException("brokerAuth password must not be null or blank");
        }
        return new BrokerAuthConfig(mechanism, username, UserConfig.resolveEnvVars(password, envLookup));
    }
}
