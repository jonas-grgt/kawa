package io.jonasg.kawa.config;

import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record UserConfig(
        String mechanism,
        String password
) {

    private static final Pattern ENV_VAR_PATTERN =
            Pattern.compile("\\$\\{([^}:]+)(?::-(.+?))?\\}");

    public UserConfig {
        if (password == null || password.isBlank()) {
            throw new IllegalArgumentException("password must not be null or blank");
        }
        password = resolveEnvVars(password, System::getenv);
    }

    static UserConfig of(
            String mechanism,
            String password,
            Function<String, String> envLookup
    ) {
        if (password == null || password.isBlank()) {
            throw new IllegalArgumentException("password must not be null or blank");
        }
        return new UserConfig(mechanism, resolveEnvVars(password, envLookup));
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
