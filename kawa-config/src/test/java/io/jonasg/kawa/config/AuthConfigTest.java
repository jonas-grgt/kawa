package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AuthConfigTest {

    @Test
    void userWithoutMechanismInheritsGlobalMechanism() {
        // given
        Map<String, UserConfig> users = Map.of("alice", new UserConfig(null, "secret"));

        // when
        AuthConfig config = new AuthConfig(Set.of("PLAIN"), users, null);

        // then
        assertThat(config.users().get("alice").mechanism()).isEqualTo("PLAIN");
    }

    @Test
    void userWithoutMechanismFailsWhenNoGlobalMechanismIsConfigured() {
        // given
        Map<String, UserConfig> users = Map.of("alice", new UserConfig(null, "secret"));

        // when / then
        assertThatThrownBy(() -> new AuthConfig(Set.of(), users, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("alice")
                .hasMessageContaining("mechanism");
    }

    @Test
    void explicitUserMechanismOverridesGlobalMechanism() {
        // given
        Map<String, UserConfig> users = Map.of("bob", new UserConfig("SCRAM-SHA-256", "secret"));

        // when
        AuthConfig config = new AuthConfig(Set.of("PLAIN", "SCRAM-SHA-256"), users, null);

        // then
        assertThat(config.users().get("bob").mechanism()).isEqualTo("SCRAM-SHA-256");
    }

    @Test
    void rejectsUserMechanismNotInAdvertisedList() {
        // given
        Map<String, UserConfig> users = Map.of(
                "bob", new UserConfig("SCRAM-SHA-256", "secret"));

        // when / then
        assertThatThrownBy(() -> new AuthConfig(Set.of("PLAIN"), users, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bob")
                .hasMessageContaining("SCRAM-SHA-256")
                .hasMessageContaining("PLAIN");
    }
}
