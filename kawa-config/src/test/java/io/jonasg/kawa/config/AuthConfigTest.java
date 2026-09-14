package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AuthConfigTest {

    @Test
    void clientWithoutMechanismInheritsGlobalMechanism() {
        // given
        Map<String, ClientConfig> clients = Map.of("alice", new ClientConfig(null, "secret"));

        // when
        AuthConfig config = new AuthConfig(Set.of("PLAIN"), clients, null);

        // then
        assertThat(config.clients().get("alice").mechanism()).isEqualTo("PLAIN");
    }

    @Test
    void clientWithoutMechanismFailsWhenNoGlobalMechanismIsConfigured() {
        // given
        Map<String, ClientConfig> clients = Map.of("alice", new ClientConfig(null, "secret"));

        // when / then
        assertThatThrownBy(() -> new AuthConfig(Set.of(), clients, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("alice")
                .hasMessageContaining("mechanism");
    }

    @Test
    void explicitClientMechanismOverridesGlobalMechanism() {
        // given
        Map<String, ClientConfig> clients = Map.of("bob", new ClientConfig("SCRAM-SHA-256", "secret"));

        // when
        AuthConfig config = new AuthConfig(Set.of("PLAIN", "SCRAM-SHA-256"), clients, null);

        // then
        assertThat(config.clients().get("bob").mechanism()).isEqualTo("SCRAM-SHA-256");
    }

    @Test
    void rejectsClientMechanismNotInAdvertisedList() {
        // given
        Map<String, ClientConfig> clients = Map.of(
                "bob", new ClientConfig("SCRAM-SHA-256", "secret"));

        // when / then
        assertThatThrownBy(() -> new AuthConfig(Set.of("PLAIN"), clients, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bob")
                .hasMessageContaining("SCRAM-SHA-256")
                .hasMessageContaining("PLAIN");
    }

    @Test
    void withClientAddsNewClient() {
        // given
        var config = new AuthConfig(Set.of("PLAIN"), Map.of(), null);

        // when
        var updated = config.withClient("alice", new ClientConfig("PLAIN", "secret"));

        // then
        assertThat(updated.clients()).containsKey("alice");
        assertThat(updated.clients().get("alice").mechanism()).isEqualTo("PLAIN");
    }

    @Test
    void withClientOverwritesExisting() {
        // given
        var config = new AuthConfig(Set.of("PLAIN"),
                Map.of("alice", new ClientConfig("PLAIN", "old-secret")), null);

        // when
        var updated = config.withClient("alice", new ClientConfig("PLAIN", "new-secret"));

        // then
        assertThat(updated.clients()).hasSize(1);
        assertThat(updated.clients().get("alice").password()).isEqualTo("new-secret");
    }

    @Test
    void withoutClientRemovesExisting() {
        // given
        var config = new AuthConfig(Set.of("PLAIN"),
                Map.of("alice", new ClientConfig("PLAIN", "secret")), null);

        // when
        var updated = config.withoutClient("alice");

        // then
        assertThat(updated.clients()).isEmpty();
    }

    @Test
    void withoutClientPreservesOtherClients() {
        // given
        var config = new AuthConfig(Set.of("PLAIN"), Map.of(
                "alice", new ClientConfig("PLAIN", "secret-alice"),
                "bob", new ClientConfig("PLAIN", "secret-bob")), null);

        // when
        var updated = config.withoutClient("alice");

        // then
        assertThat(updated.clients()).hasSize(1);
        assertThat(updated.clients()).containsKey("bob");
    }

    @Test
    void withClientPreservesMechanisms() {
        // given
        var config = new AuthConfig(Set.of("PLAIN", "SCRAM-SHA-256"), Map.of(), null);

        // when
        var updated = config.withClient("alice", new ClientConfig("PLAIN", "secret"));

        // then
        assertThat(updated.mechanisms()).containsExactlyInAnyOrder("PLAIN", "SCRAM-SHA-256");
    }

    @Test
    void withClientAddsMechanismWhenClientUsesNewOne() {
        // given
        var config = new AuthConfig(Set.of("PLAIN"), Map.of(), null);

        // when
        var updated = config.withClient("bob", new ClientConfig("SCRAM-SHA-256", "secret"));

        // then
        assertThat(updated.mechanisms()).containsExactlyInAnyOrder("PLAIN", "SCRAM-SHA-256");
        assertThat(updated.clients().get("bob").mechanism()).isEqualTo("SCRAM-SHA-256");
    }

    @Test
    void withClientWithoutMechanismIsRejected() {
        // given
        var config = new AuthConfig(Set.of(), Map.of(), null);

        // when / then
        assertThatThrownBy(() -> config.withClient("alice", new ClientConfig(null, "secret")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("alice")
                .hasMessageContaining("mechanism");
    }

    @Test
    void withClientWithoutMechanismIsRejectedEvenWhenMechanismsConfigured() {
        // given
        var config = new AuthConfig(Set.of("SCRAM-SHA-256", "PLAIN"), Map.of(), null);

        // when / then
        assertThatThrownBy(() -> config.withClient("alice", new ClientConfig(null, "secret")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("alice")
                .hasMessageContaining("mechanism");
    }

    @Test
    void withClientRejectsNullClient() {
        // given
        var config = new AuthConfig(Set.of("PLAIN"), Map.of(), null);

        // when / then
        assertThatThrownBy(() -> config.withClient("alice", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("alice");
    }

    @Test
    void brokerAuthPreservedThroughWithClient() {
        // given
        var brokerAuth = BrokerAuthConfig.of("PLAIN", "admin", "kafka-secret", k -> null);
        var config = new AuthConfig(Set.of("PLAIN"),
                Map.of("alice", new ClientConfig("PLAIN", "secret")), brokerAuth);

        // when
        var updated = config.withClient("bob", new ClientConfig("PLAIN", "bob-secret"));

        // then
        assertThat(updated.brokerAuth()).isEqualTo(brokerAuth);
        assertThat(updated.clients()).hasSize(2);
    }
}