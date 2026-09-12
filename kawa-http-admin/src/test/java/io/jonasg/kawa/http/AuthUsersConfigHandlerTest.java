package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.UserConfig;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AuthUsersConfigHandlerTest {

    @Test
    void getListsConfiguredUsers() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().withUser("alice", new UserConfig("PLAIN", "secret"))));
        var handler = new AuthUsersConfigHandler(repository);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("GET", "/config/auth/users", Map.of(), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.body()).isEqualTo(Map.of("alice", new UserConfig("PLAIN", "secret")));
    }

    @Test
    void getReturnsEmptyMapWhenNoSnapshotApplied() {
        // given
        var handler = new AuthUsersConfigHandler(new FakeGatewayConfigRepository(null));

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("GET", "/config/auth/users", Map.of(), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.body()).isEqualTo(Map.of());
    }

    @Test
    void putAddsUserAndPersistsSnapshot() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty());
        var handler = new AuthUsersConfigHandler(repository);
        byte[] body = "{\"mechanism\":\"PLAIN\",\"password\":\"secret\"}".getBytes(StandardCharsets.UTF_8);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/config/auth/users/alice", Map.of("name", "alice"), body));

        // then
        assertThat(response.status()).isEqualTo(200);
        assertThat(repository.getActiveConfig().auth().users()).containsKey("alice");
        assertThat(repository.getActiveConfig().auth().users().get("alice").password()).isEqualTo("secret");
    }

    @Test
    void putWithoutMechanismIsRejected() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty());
        var handler = new AuthUsersConfigHandler(repository);
        byte[] body = "{\"password\":\"secret\"}".getBytes(StandardCharsets.UTF_8);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/config/auth/users/alice", Map.of("name", "alice"), body));

        // then
        assertThat(response.status()).isEqualTo(400);
        assertThat(repository.getActiveConfig().auth().users()).isEmpty();
    }

    @Test
    void putRejectsInvalidBody() {
        // given
        var handler = new AuthUsersConfigHandler(new FakeGatewayConfigRepository(GatewayConfig.empty()));

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/config/auth/users/alice", Map.of("name", "alice"),
                        "not json".getBytes(StandardCharsets.UTF_8)));

        // then
        assertThat(response.status()).isEqualTo(400);
    }

    @Test
    void deleteRemovesUserAndPersistsSnapshot() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .updateAuth(GatewayConfig.empty().auth().withUser("alice", new UserConfig("PLAIN", "secret"))));
        var handler = new AuthUsersConfigHandler(repository);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("DELETE", "/config/auth/users/alice", Map.of("name", "alice"), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(204);
        assertThat(repository.getActiveConfig().auth().users()).isEmpty();
    }

    @Test
    void deleteMissingUserReturnsNotFound() {
        // given
        var handler = new AuthUsersConfigHandler(new FakeGatewayConfigRepository(GatewayConfig.empty()));

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("DELETE", "/config/auth/users/alice", Map.of("name", "alice"), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(404);
    }
}