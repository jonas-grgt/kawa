package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.RoleConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// Slice tests for the `/rbac/roles` admin surface: real HTTP requests through a booted
/// [AdminHttpServer], asserting the JSON wire format the admin UI consumes.
class RoleSliceTest extends AdminHttpSliceTestBase {

    @Test
    void listsConfiguredRoles() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac().withRole("reader", new RoleConfig(null))));
        startServer();

        // when
        var response = send("GET", "/rbac/roles", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"name\":\"reader\"", "\"acls\":[]");
    }

    @Test
    void listsRolesSortedByName() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac()
                        .withRole("writer", new RoleConfig(null))
                        .withRole("reader", new RoleConfig(null))));
        startServer();

        // when
        var response = send("GET", "/rbac/roles", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body().indexOf("\"reader\"")).isLessThan(response.body().indexOf("\"writer\""));
    }

    @Test
    void listsRolesEmptyWhenNoSnapshotApplied() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(null);
        startServer();

        // when
        var response = send("GET", "/rbac/roles", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("[]");
    }

    @Test
    void addsRoleAndPersistsSnapshot() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/rbac/roles/reader",
                "{\"acls\":[{\"resource\":{\"type\":\"TOPIC\",\"pattern\":\"orders\"},\"operation\":\"READ\"}]}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().rbac().roles()).containsKey("reader");
        assertThat(repository.getActiveConfig().rbac().roles().get("reader").acls()).hasSize(1);
    }

    @Test
    void rejectsInvalidRoleBody() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/rbac/roles/reader", "not json");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().rbac().roles()).isEmpty();
    }

    @Test
    void removesRoleAndPersistsSnapshot() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac().withRole("reader", new RoleConfig(null))));
        startServer();

        // when
        var response = send("DELETE", "/rbac/roles/reader", null);

        // then
        assertThat(response.statusCode()).isEqualTo(204);
        assertThat(repository.getActiveConfig().rbac().roles()).isEmpty();
    }

    @Test
    void missingRoleReturnsNotFound() throws Exception {
        // given
        startServer();

        // when
        var response = send("DELETE", "/rbac/roles/reader", null);

        // then
        assertThat(response.statusCode()).isEqualTo(404);
    }
}