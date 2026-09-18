package io.jonasg.kawa.config;

import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record ClientConfig(
        String mechanism,
        HashedPassword password
) {

    private static final Pattern ENV_VAR_PATTERN =
            Pattern.compile("\\$\\{([^}:]+)(?::-(.+?))?\\}");

    public ClientConfig {
        if (password == null) {
            throw new IllegalArgumentException("password must not be null");
        }
    }

    public ClientConfig(String mechanism, String password) {
        this(mechanism, HashedPassword.fromEncoded(resolvePassword(password, System::getenv)));
    }

    static ClientConfig of(
            String mechanism,
            String password,
            Function<String, String> envLookup
    ) {
        if (password == null || password.isBlank()) {
            throw new IllegalArgumentException("password must not be null or blank");
        }
        return new ClientConfig(
                mechanism,
                HashedPassword.fromEncoded(resolveEnvVars(password, envLookup)));
    }

    private static String resolvePassword(String password, Function<String, String> envLookup) {
        if (password == null || password.isBlank()) {
            throw new IllegalArgumentException("password must not be null or blank");
        }
        return resolveEnvVars(password, envLookup);
    }

    static String resolveEnvVars(String value, Function<String, String> envLookup) {
        Matcher matcher = ENV_VAR_PATTERN.matcher(value);
        if (!matcher.find()) {
            return value;
        }
        matcher.reset();
        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            String varName = matcher.group(1);
            String defaultValue = matcher.group(2);
            String resolved = envLookup.apply(varName);
            if (resolved == null) {
                if (defaultValue != null) {
                    resolved = defaultValue;
                } else {
                    throw new IllegalArgumentException(
                            "Environment variable '" + varName
                                    + "' is not set and no default is configured");
                }
            }
            matcher.appendReplacement(result, Matcher.quoteReplacement(resolved));
        }
        matcher.appendTail(result);
        return result.toString();
    }
}
