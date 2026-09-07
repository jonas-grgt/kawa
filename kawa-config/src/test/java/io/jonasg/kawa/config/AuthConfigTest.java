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

    @Test
    void withUserAddsNewUser() {
        // given
        var config = new AuthConfig(Set.of("PLAIN"), Map.of(), null);

        // when
        var updated = config.withUser("alice", new UserConfig("PLAIN", "secret"));

        // then
        assertThat(updated.users()).containsKey("alice");
        assertThat(updated.users().get("alice").mechanism()).isEqualTo("PLAIN");
    }

    @Test
    void withUserOverwritesExisting() {
        // given
        var config = new AuthConfig(Set.of("PLAIN"),
                Map.of("alice", new UserConfig("PLAIN", "old-secret")), null);

        // when
        var updated = config.withUser("alice", new UserConfig("PLAIN", "new-secret"));

        // then
        assertThat(updated.users()).hasSize(1);
        assertThat(updated.users().get("alice").password()).isEqualTo("new-secret");
    }

    @Test
    void withoutUserRemovesExisting() {
        // given
        var config = new AuthConfig(Set.of("PLAIN"),
                Map.of("alice", new UserConfig("PLAIN", "secret")), null);

        // when
        var updated = config.withoutUser("alice");

        // then
        assertThat(updated.users()).isEmpty();
    }

    @Test
    void withoutUserPreservesOtherUsers() {
        // given
        var config = new AuthConfig(Set.of("PLAIN"), Map.of(
                "alice", new UserConfig("PLAIN", "secret-alice"),
                "bob", new UserConfig("PLAIN", "secret-bob")), null);

        // when
        var updated = config.withoutUser("alice");

        // then
        assertThat(updated.users()).hasSize(1);
        assertThat(updated.users()).containsKey("bob");
    }

    @Test
    void withUserPreservesMechanisms() {
        // given
        var config = new AuthConfig(Set.of("PLAIN", "SCRAM-SHA-256"), Map.of(), null);

        // when
        var updated = config.withUser("alice", new UserConfig("PLAIN", "secret"));

        // then
        assertThat(updated.mechanisms()).containsExactlyInAnyOrder("PLAIN", "SCRAM-SHA-256");
    }

    @Test
    void withUserAddsMechanismWhenUserUsesNewOne() {
        // given
        var config = new AuthConfig(Set.of("PLAIN"), Map.of(), null);

        // when
        var updated = config.withUser("bob", new UserConfig("SCRAM-SHA-256", "secret"));

        // then
        assertThat(updated.mechanisms()).containsExactlyInAnyOrder("PLAIN", "SCRAM-SHA-256");
        assertThat(updated.users().get("bob").mechanism()).isEqualTo("SCRAM-SHA-256");
    }

    @Test
    void withUserWithoutMechanismIsRejected() {
        // given
        var config = new AuthConfig(Set.of(), Map.of(), null);

        // when / then
        assertThatThrownBy(() -> config.withUser("alice", new UserConfig(null, "secret")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("alice")
                .hasMessageContaining("mechanism");
    }

    @Test
    void withUserWithoutMechanismIsRejectedEvenWhenMechanismsConfigured() {
        // given
        var config = new AuthConfig(Set.of("SCRAM-SHA-256", "PLAIN"), Map.of(), null);

        // when / then
        assertThatThrownBy(() -> config.withUser("alice", new UserConfig(null, "secret")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("alice")
                .hasMessageContaining("mechanism");
    }

    @Test
    void withUserRejectsNullUser() {
        // given
        var config = new AuthConfig(Set.of("PLAIN"), Map.of(), null);

        // when / then
        assertThatThrownBy(() -> config.withUser("alice", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("alice");
    }

    @Test
    void brokerAuthPreservedThroughWithUser() {
        // given
        var brokerAuth = BrokerAuthConfig.of("PLAIN", "admin", "kafka-secret", k -> null);
        var config = new AuthConfig(Set.of("PLAIN"),
                Map.of("alice", new UserConfig("PLAIN", "secret")), brokerAuth);

        // when
        var updated = config.withUser("bob", new UserConfig("PLAIN", "bob-secret"));

        // then
        assertThat(updated.brokerAuth()).isEqualTo(brokerAuth);
        assertThat(updated.users()).hasSize(2);
    }
}
