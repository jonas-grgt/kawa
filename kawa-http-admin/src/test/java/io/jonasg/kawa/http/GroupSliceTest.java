package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GroupConfig;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/// Slice tests for the `/rbac/groups` admin surface: real HTTP requests through a booted
/// [AdminHttpServer], asserting the JSON wire format the admin UI consumes.
class GroupSliceTest extends AdminHttpSliceTestBase {

    @Test
    void listsConfiguredGroups() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac().withGroup("producers", new GroupConfig(null, null))));
        startServer();

        // when
        var response = send("GET", "/rbac/groups", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"name\":\"producers\"", "\"clients\":[]", "\"roles\":[]");
    }

    @Test
    void listsGroupsSortedByName() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac()
                        .withGroup("writers", new GroupConfig(null, null))
                        .withGroup("producers", new GroupConfig(null, null))));
        startServer();

        // when
        var response = send("GET", "/rbac/groups", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body().indexOf("\"producers\"")).isLessThan(response.body().indexOf("\"writers\""));
    }

    @Test
    void listsGroupsEmptyWhenNoSnapshotApplied() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(null);
        startServer();

        // when
        var response = send("GET", "/rbac/groups", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("[]");
    }

    @Test
    void addsGroupAndPersistsSnapshot() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/rbac/groups/producers", "{\"clients\":[\"alice\"],\"roles\":[\"reader\"]}");

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().rbac().groups()).containsKey("producers");
        assertThat(repository.getActiveConfig().rbac().groups().get("producers").clients())
                .containsExactly("alice");
    }

    @Test
    void rejectsInvalidGroupBody() throws Exception {
        // given
        startServer();

        // when
        var response = send("PUT", "/rbac/groups/producers", "not json");

        // then
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(repository.getActiveConfig().rbac().groups()).isEmpty();
    }

    @Test
    void removesGroupAndPersistsSnapshot() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac().withGroup("producers", new GroupConfig(null, null))));
        startServer();

        // when
        var response = send("DELETE", "/rbac/groups/producers", null);

        // then
        assertThat(response.statusCode()).isEqualTo(204);
        assertThat(repository.getActiveConfig().rbac().groups()).isEmpty();
    }

    @Test
    void rejectsRemovalWhileGroupHasClients() throws Exception {
        // given
        repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac()
                        .withGroup("producers", new GroupConfig(List.of("alice"), List.of("reader")))));
        startServer();

        // when
        var response = send("DELETE", "/rbac/groups/producers", null);

        // then
        assertThat(response.statusCode()).isEqualTo(409);
        assertThat(response.body()).contains("still has clients", "alice");
        assertThat(repository.getActiveConfig().rbac().groups()).containsKey("producers");
    }

    @Test
    void missingGroupReturnsNotFound() throws Exception {
        // given
        startServer();

        // when
        var response = send("DELETE", "/rbac/groups/producers", null);

        // then
        assertThat(response.statusCode()).isEqualTo(404);
    }
}