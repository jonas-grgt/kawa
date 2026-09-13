package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.UserConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// Slice tests for the `/auth/users` admin surface: real HTTP requests through a booted
/// [AdminHttpServer], asserting the JSON wire format the admin UI consumes.
class UserSliceTest extends AdminHttpSliceTestBase {

    @Test
    void listsConfiguredUsers() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().withUser("alice", new UserConfig("PLAIN", "secret"))));
        startServer();

        // when
        var response = send("GET", "/auth/users", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"username\":\"alice\"", "\"PLAIN\"");
    }

    @Test
    void listsUsersSortedByUsername() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth()
                        .withUser("bob", new UserConfig("PLAIN", "bob-secret"))
                        .withUser("alice", new UserConfig("PLAIN", "alice-secret"))));
        startServer();

        // when
        var response = send("GET", "/auth/users", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body().indexOf("\"alice\"")).isLessThan(response.body().indexOf("\"bob\""));
    }

    @Test
    void listsUsersEmptyWhenNoSnapshotApplied() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(null);
        startServer();

        // when
        var response = send("GET", "/auth/users", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("[]");
    }

    @Test
    void addsUserAndPersistsSnapshot() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/auth/users/alice", "{\"mechanism\":\"PLAIN\",\"password\":\"secret\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().auth().users()).containsKey("alice");
        assertThat(repository.getActiveConfig().auth().users().get("alice").password()).isEqualTo("secret");
    }

    @Test
    void rejectsUserWithoutMechanism() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/auth/users/alice", "{\"password\":\"secret\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().auth().users()).isEmpty();
    }

    @Test
    void rejectsInvalidUserBody() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/auth/users/alice", "not json");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().auth().users()).isEmpty();
    }

    @Test
    void removesUserAndPersistsSnapshot() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().withUser("alice", new UserConfig("PLAIN", "secret"))));
        startServer();

        // when
        var response = send("DELETE", "/auth/users/alice", null);

        // then
        assertThat(response.statusCode()).isEqualTo(204);
        assertThat(repository.getActiveConfig().auth().users()).isEmpty();
    }

    @Test
    void missingUserReturnsNotFound() throws Exception {
        // given
        startServer();

        // when
        var response = send("DELETE", "/auth/users/alice", null);

        // then
        assertThat(response.statusCode()).isEqualTo(404);
    }

    @Test
    void patchChangesMechanismAndPreservesPassword() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().withUser("alice", new UserConfig("PLAIN", "secret"))));
        startServer();

        // when
        var response = send("PATCH", "/auth/users/alice", "{\"mechanism\":\"SCRAM-SHA-256\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().auth().users().get("alice"))
                .isEqualTo(new UserConfig("SCRAM-SHA-256", "secret"));
    }

    @Test
    void patchChangesPasswordAndPreservesMechanism() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().withUser("alice", new UserConfig("PLAIN", "secret"))));
        startServer();

        // when
        var response = send("PATCH", "/auth/users/alice", "{\"password\":\"new-secret\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().auth().users().get("alice"))
                .isEqualTo(new UserConfig("PLAIN", "new-secret"));
    }

    @Test
    void patchUnknownUserReturnsNotFound() throws Exception {
        // given
        startServer();

        // when
        var response = send("PATCH", "/auth/users/alice", "{\"mechanism\":\"SCRAM-SHA-256\"}");

        // then
        assertThat(response.statusCode()).isEqualTo(404);
    }

    @Test
    void patchWithNothingToPatchIsRejected() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().withUser("alice", new UserConfig("PLAIN", "secret"))));
        startServer();

        // when
        var response = send("PATCH", "/auth/users/alice", "{}");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("no fields to patch");
        assertThat(repository.getActiveConfig().auth().users().get("alice"))
                .isEqualTo(new UserConfig("PLAIN", "secret"));
    }

    @Test
    void patchRejectsInvalidBody() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().withUser("alice", new UserConfig("PLAIN", "secret"))));
        startServer();

        // when
        var response = send("PATCH", "/auth/users/alice", "not json");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).startsWith("{\"error\":\"invalid user body");
    }
}