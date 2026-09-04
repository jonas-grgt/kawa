package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UserConfigTest {

    private static final Function<String, String> ENV = Map.of(
            "MY_SECRET", "s3cret-from-env",
            "MY_PORT", "5432"
    )::get;

    private static final Function<String, String> EMPTY_ENV = k -> null;

    @Test
    void allowsMissingMechanismForGlobalInheritance() {
        UserConfig config = new UserConfig(null, "secret");

        assertThat(config.mechanism()).isNull();
        assertThat(config.password()).isEqualTo("secret");
    }

    @Test
    void rejectsNullPassword() {
        assertThatThrownBy(() -> new UserConfig("PLAIN", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("password");
    }

    @Test
    void rejectsBlankPassword() {
        assertThatThrownBy(() -> new UserConfig("PLAIN", "   "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("password");
    }

    @Test
    void resolvesEnvironmentVariable() {
        UserConfig config = UserConfig.of("PLAIN", "${MY_SECRET}", ENV);

        assertThat(config.password()).isEqualTo("s3cret-from-env");
    }

    @Test
    void resolvesDefaultWhenEnvVarIsMissing() {
        UserConfig config = UserConfig.of("PLAIN", "${NONEXISTENT:-fallback}", EMPTY_ENV);

        assertThat(config.password()).isEqualTo("fallback");
    }

    @Test
    void rejectsUnresolvedEnvVarWithoutDefault() {
        assertThatThrownBy(() -> UserConfig.of("PLAIN", "${NONEXISTENT}", EMPTY_ENV))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("NONEXISTENT");
    }

    @Test
    void passesLiteralPasswordThroughUnchanged() {
        UserConfig config = UserConfig.of("PLAIN", "plain-password", EMPTY_ENV);

        assertThat(config.password()).isEqualTo("plain-password");
    }

    @Test
    void publicConstructorUsesSystemGetenv() {
        UserConfig config = new UserConfig("PLAIN", "literal");

        assertThat(config.password()).isEqualTo("literal");
    }
}
