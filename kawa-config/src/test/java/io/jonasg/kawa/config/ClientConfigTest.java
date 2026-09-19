package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ClearEnvironmentVariable;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClientConfigTest {

    @Test
    void allowsMissingMechanismForGlobalInheritance() {
        ClientConfig config = new ClientConfig(null, HashedPassword.fromEncoded("secret"));

        assertThat(config.mechanism()).isNull();
        assertThat(config.password().encoded()).isEqualTo("secret");
    }

    @Test
    void rejectsNullPassword() {
        assertThatThrownBy(() -> new ClientConfig("PLAIN", (HashedPassword) null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("password");
    }

    @Test
    void rejectsBlankPassword() {
        assertThatThrownBy(() -> new ClientConfig("PLAIN", "   "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("password");
    }

    @Test
    @SetEnvironmentVariable(key = "KAWA_TEST_CLIENT_PASSWORD", value = "s3cret-from-env")
    void resolvesEnvironmentVariable() {
        ClientConfig config = new ClientConfig("PLAIN", "${KAWA_TEST_CLIENT_PASSWORD}");

        assertThat(config.password().encoded()).isEqualTo("s3cret-from-env");
    }

    @Test
    @ClearEnvironmentVariable(key = "KAWA_TEST_MISSING_PASSWORD")
    void resolvesDefaultWhenEnvVarIsMissing() {
        ClientConfig config = new ClientConfig("PLAIN", "${KAWA_TEST_MISSING_PASSWORD:-fallback}");

        assertThat(config.password().encoded()).isEqualTo("fallback");
    }

    @Test
    @ClearEnvironmentVariable(key = "KAWA_TEST_MISSING_PASSWORD")
    void rejectsUnresolvedEnvVarWithoutDefault() {
        assertThatThrownBy(() -> new ClientConfig("PLAIN", "${KAWA_TEST_MISSING_PASSWORD}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("KAWA_TEST_MISSING_PASSWORD");
    }

    @Test
    void passesLiteralPasswordThroughUnchanged() {
        ClientConfig config = new ClientConfig("PLAIN", "plain-password");

        assertThat(config.password().encoded()).isEqualTo("plain-password");
    }

    @Test
    void publicConstructorUsesSystemGetenv() {
        ClientConfig config = new ClientConfig("PLAIN", "literal");

        assertThat(config.password().encoded()).isEqualTo("literal");
    }
}
