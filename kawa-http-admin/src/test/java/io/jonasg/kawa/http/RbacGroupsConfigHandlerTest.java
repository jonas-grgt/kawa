package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GroupConfig;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RbacGroupsConfigHandlerTest {

    @Test
    void getListsConfiguredGroups() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac().withGroup("producers", new GroupConfig(null, null))));
        var handler = new RbacGroupsConfigHandler(repository);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("GET", "/config/rbac/groups", Map.of(), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.body()).isEqualTo(List.of(new GroupView("producers", List.of(), List.of())));
    }

    @Test
    void getListsGroupsSortedByName() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac()
                        .withGroup("writers", new GroupConfig(null, null))
                        .withGroup("producers", new GroupConfig(null, null))));
        var handler = new RbacGroupsConfigHandler(repository);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("GET", "/config/rbac/groups", Map.of(), new byte[0]));

        // then
        assertThat(response.body()).isEqualTo(List.of(
                new GroupView("producers", List.of(), List.of()),
                new GroupView("writers", List.of(), List.of())));
    }

    @Test
    void getReturnsEmptyListWhenNoSnapshotApplied() {
        // given
        var handler = new RbacGroupsConfigHandler(new FakeGatewayConfigRepository(null));

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("GET", "/config/rbac/groups", Map.of(), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.body()).isEqualTo(List.of());
    }

    @Test
    void putAddsGroupAndPersistsSnapshot() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty());
        var handler = new RbacGroupsConfigHandler(repository);
        byte[] body = "{\"members\":[\"alice\"],\"roles\":[\"reader\"]}".getBytes(StandardCharsets.UTF_8);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/config/rbac/groups/producers", Map.of("name", "producers"), body));

        // then
        assertThat(response.status()).isEqualTo(200);
        assertThat(repository.getActiveConfig().rbac().groups()).containsKey("producers");
        assertThat(repository.getActiveConfig().rbac().groups().get("producers").members())
                .containsExactly("alice");
    }

    @Test
    void putRejectsInvalidBody() {
        // given
        var handler = new RbacGroupsConfigHandler(new FakeGatewayConfigRepository(GatewayConfig.empty()));

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/config/rbac/groups/producers", Map.of("name", "producers"),
                        "not json".getBytes(StandardCharsets.UTF_8)));

        // then
        assertThat(response.status()).isEqualTo(400);
    }

    @Test
    void deleteRemovesGroupAndPersistsSnapshot() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateRbac(GatewayConfig.empty().rbac().withGroup("producers", new GroupConfig(null, null))));
        var handler = new RbacGroupsConfigHandler(repository);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("DELETE", "/config/rbac/groups/producers", Map.of("name", "producers"), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(204);
        assertThat(repository.getActiveConfig().rbac().groups()).isEmpty();
    }

    @Test
    void deleteMissingGroupReturnsNotFound() {
        // given
        var handler = new RbacGroupsConfigHandler(new FakeGatewayConfigRepository(GatewayConfig.empty()));

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("DELETE", "/config/rbac/groups/producers", Map.of("name", "producers"), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(404);
    }
}