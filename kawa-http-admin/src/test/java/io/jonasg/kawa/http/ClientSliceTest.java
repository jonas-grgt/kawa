package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.ClientConfig;
import io.jonasg.kawa.config.GroupConfig;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/// Slice tests for the `/auth/clients` admin surface: real HTTP requests through a booted
/// [AdminHttpServer], asserting the JSON wire format the admin UI consumes.
class ClientSliceTest extends AdminHttpSliceTestBase {

    @Test
    void listsConfiguredClients() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().withClient("alice", new ClientConfig("PLAIN", "secret"))));
        startServer();

        // when
        var response = send("GET", "/auth/clients", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"username\":\"alice\"", "\"PLAIN\"");
    }

    @Test
    void listsClientsSortedByUsername() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth()
                        .withClient("bob", new ClientConfig("PLAIN", "bob-secret"))
                        .withClient("alice", new ClientConfig("PLAIN", "alice-secret"))));
        startServer();

        // when
        var response = send("GET", "/auth/clients", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body().indexOf("\"alice\"")).isLessThan(response.body().indexOf("\"bob\""));
    }

    @Test
    void listsClientsEmptyWhenNoSnapshotApplied() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(null);
        startServer();

        // when
        var response = send("GET", "/auth/clients", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("[]");
    }

    @Test
    void addsClientAndPersistsSnapshot() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/auth/clients/alice", "{\"mechanism\":\"PLAIN\",\"password\":\"secret\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().auth().clients()).containsKey("alice");
        assertThat(repository.getActiveConfig().auth().clients().get("alice").password()).isEqualTo("secret");
        assertThat(repository.updateCalls()).isEqualTo(1);
        assertThat(repository.updateAndWaitCalls()).isEqualTo(0);
    }

    @Test
    void addsClientWithAppliedConsistencyWaitsForApplyMode() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/auth/clients/alice?consistency=applied", "{\"mechanism\":\"PLAIN\",\"password\":\"secret\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.updateCalls()).isEqualTo(0);
        assertThat(repository.updateAndWaitCalls()).isEqualTo(1);
    }

    @Test
    void addsClientWithPersistedConsistencyUsesPersistedMode() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/auth/clients/alice?consistency=persisted", "{\"mechanism\":\"PLAIN\",\"password\":\"secret\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.updateCalls()).isEqualTo(1);
        assertThat(repository.updateAndWaitCalls()).isEqualTo(0);
    }

    @Test
    void rejectsInvalidConsistencyOnClientWrite() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/auth/clients/alice?consistency=strong", "{\"mechanism\":\"PLAIN\",\"password\":\"secret\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("invalid consistency 'strong'");
        assertThat(repository.updateCalls()).isEqualTo(0);
        assertThat(repository.updateAndWaitCalls()).isEqualTo(0);
    }

    @Test
    void rejectsClientWithoutMechanism() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/auth/clients/alice", "{\"password\":\"secret\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().auth().clients()).isEmpty();
    }

    @Test
    void rejectsInvalidClientBody() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/auth/clients/alice", "not json");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().auth().clients()).isEmpty();
    }

    @Test
    void removesClientAndPersistsSnapshot() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().withClient("alice", new ClientConfig("PLAIN", "secret"))));
        startServer();

        // when
        var response = send("DELETE", "/auth/clients/alice", null);

        // then
        assertThat(response.statusCode()).isEqualTo(204);
        assertThat(repository.getActiveConfig().auth().clients()).isEmpty();
    }

    @Test
    void removesClientNotReferencedByAnyGroup() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().withClient("alice", new ClientConfig("PLAIN", "secret")))
                .updateRbac(GatewayConfig.empty().rbac()
                        .withGroup("producers", new GroupConfig(List.of("bob"), List.of("reader")))));
        startServer();

        // when
        var response = send("DELETE", "/auth/clients/alice", null);

        // then
        assertThat(response.statusCode()).isEqualTo(204);
        assertThat(repository.getActiveConfig().auth().clients()).isEmpty();
    }

    @Test
    void rejectsRemovalWhileClientIsInGroup() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().withClient("alice", new ClientConfig("PLAIN", "secret")))
                .updateRbac(GatewayConfig.empty().rbac()
                        .withGroup("producers", new GroupConfig(List.of("alice"), List.of("reader")))));
        startServer();

        // when
        var response = send("DELETE", "/auth/clients/alice", null);

        // then
        assertThat(response.statusCode()).isEqualTo(409);
        assertThat(response.body()).contains("still in groups", "producers");
        assertThat(repository.getActiveConfig().auth().clients()).containsKey("alice");
    }

    @Test
    void missingClientReturnsNotFound() throws Exception {
        // given
        startServer();

        // when
        var response = send("DELETE", "/auth/clients/alice", null);

        // then
        assertThat(response.statusCode()).isEqualTo(404);
    }

    @Test
    void patchChangesMechanismAndPreservesPassword() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().withClient("alice", new ClientConfig("PLAIN", "secret"))));
        startServer();

        // when
        var response = send("PATCH", "/auth/clients/alice", "{\"mechanism\":\"SCRAM-SHA-256\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().auth().clients().get("alice"))
                .isEqualTo(new ClientConfig("SCRAM-SHA-256", "secret"));
    }

    @Test
    void patchWithAppliedConsistencyWaitsForApplyMode() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().withClient("alice", new ClientConfig("PLAIN", "secret"))));
        startServer();

        // when
        var response = send("PATCH", "/auth/clients/alice?consistency=applied", "{\"mechanism\":\"SCRAM-SHA-256\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.updateCalls()).isEqualTo(0);
        assertThat(repository.updateAndWaitCalls()).isEqualTo(1);
    }

    @Test
    void patchChangesPasswordAndPreservesMechanism() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().withClient("alice", new ClientConfig("PLAIN", "secret"))));
        startServer();

        // when
        var response = send("PATCH", "/auth/clients/alice", "{\"password\":\"new-secret\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().auth().clients().get("alice"))
                .isEqualTo(new ClientConfig("PLAIN", "new-secret"));
    }

    @Test
    void patchUnknownClientReturnsNotFound() throws Exception {
        // given
        startServer();

        // when
        var response = send("PATCH", "/auth/clients/alice", "{\"mechanism\":\"SCRAM-SHA-256\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(404);
    }

    @Test
    void patchWithNothingToPatchIsRejected() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().withClient("alice", new ClientConfig("PLAIN", "secret"))));
        startServer();

        // when
        var response = send("PATCH", "/auth/clients/alice", "{}");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("no fields to patch");
        assertThat(repository.getActiveConfig().auth().clients().get("alice"))
                .isEqualTo(new ClientConfig("PLAIN", "secret"));
    }

    @Test
    void patchRejectsInvalidBody() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().withClient("alice", new ClientConfig("PLAIN", "secret"))));
        startServer();

        // when
        var response = send("PATCH", "/auth/clients/alice", "not json");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).startsWith("{\"error\":\"invalid client body");
    }
}
