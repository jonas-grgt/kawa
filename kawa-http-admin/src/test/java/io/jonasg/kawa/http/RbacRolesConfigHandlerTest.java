package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.RoleConfig;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RbacRolesConfigHandlerTest {

    @Test
    void getListsConfiguredRoles() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac().withRole("reader", new RoleConfig(null))));
        var handler = new RbacRolesConfigHandler(repository);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("GET", "/config/rbac/roles", Map.of(), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.body()).isEqualTo(Map.of("reader", new RoleConfig(null)));
    }

    @Test
    void getReturnsEmptyMapWhenNoSnapshotApplied() {
        // given
        var handler = new RbacRolesConfigHandler(new FakeGatewayConfigRepository(null));

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("GET", "/config/rbac/roles", Map.of(), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.body()).isEqualTo(Map.of());
    }

    @Test
    void putAddsRoleAndPersistsSnapshot() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty());
        var handler = new RbacRolesConfigHandler(repository);
        byte[] body = ("{\"acls\":[{\"resource\":{\"type\":\"TOPIC\",\"pattern\":\"orders\"},"
                + "\"operation\":\"READ\"}]}").getBytes(StandardCharsets.UTF_8);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/config/rbac/roles/reader", Map.of("name", "reader"), body));

        // then
        assertThat(response.status()).isEqualTo(200);
        assertThat(repository.getActiveConfig().rbac().roles()).containsKey("reader");
        assertThat(repository.getActiveConfig().rbac().roles().get("reader").acls()).hasSize(1);
    }

    @Test
    void putRejectsInvalidBody() {
        // given
        var handler = new RbacRolesConfigHandler(new FakeGatewayConfigRepository(GatewayConfig.empty()));

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/config/rbac/roles/reader", Map.of("name", "reader"),
                        "not json".getBytes(StandardCharsets.UTF_8)));

        // then
        assertThat(response.status()).isEqualTo(400);
    }

    @Test
    void deleteRemovesRoleAndPersistsSnapshot() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac().withRole("reader", new RoleConfig(null))));
        var handler = new RbacRolesConfigHandler(repository);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("DELETE", "/config/rbac/roles/reader", Map.of("name", "reader"), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(204);
        assertThat(repository.getActiveConfig().rbac().roles()).isEmpty();
    }

    @Test
    void deleteMissingRoleReturnsNotFound() {
        // given
        var handler = new RbacRolesConfigHandler(new FakeGatewayConfigRepository(GatewayConfig.empty()));

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("DELETE", "/config/rbac/roles/reader", Map.of("name", "reader"), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(404);
    }
}