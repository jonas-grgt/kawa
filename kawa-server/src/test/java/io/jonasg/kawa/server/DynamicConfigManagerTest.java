package io.jonasg.kawa.server;

import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.AuthConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.config.ResourceConfig;
import io.jonasg.kawa.config.RoleConfig;
import io.jonasg.kawa.config.UserConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.core.VirtualTopicManager;
import io.jonasg.kawa.rbac.RbacAuthorizer;
import io.jonasg.kawa.server.auth.AuthenticationResult;
import io.jonasg.kawa.server.auth.SaslAuthenticator;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DynamicConfigManagerTest {

    private static GatewayConfig config(
            Map<String, VirtualTopicConfig> virtualTopics,
            RbacConfig rbac,
            AuthConfig auth
    ) {
        return new GatewayConfig("test", null, null, virtualTopics, null, null, auth, rbac, null, null);
    }

    private static RbacConfig rbacAllowingReadOnOrders() {
        return new RbacConfig(
                Map.of("reader", new RoleConfig(List.of(
                        new AclConfig(new ResourceConfig(ResourceType.TOPIC, "orders", PatternType.LITERAL),
                                AclOperation.READ, AclPermissionType.ALLOW)))),
                Map.of("team", new GroupConfig(List.of("alice"), List.of("reader"))));
    }

    private static AuthConfig plainAuth() {
        return new AuthConfig(Set.of("PLAIN"), Map.of("alice", new UserConfig("PLAIN", "secret")), null);
    }

    @Test
    void appliesSnapshotToAllConsumers() {
        // given
        var virtualTopics = new VirtualTopicManager(Map.of());
        var authorizer = new RbacAuthorizer(new RbacConfig(Map.of(), Map.of()));
        var sasl = new SaslAuthenticator(Set.of());
        var manager = new DynamicConfigManager("localhost:9092", "__kawa", "test-group",
                virtualTopics, authorizer, sasl);
        var config = config(Map.of("orders", new VirtualTopicConfig("orders-v2")), rbacAllowingReadOnOrders(), plainAuth());

        // when
        manager.apply(config);

        // then
        assertThat(virtualTopics.toPhysical("orders")).isEqualTo("orders-v2");
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders", AclOperation.READ)).isTrue();
        var handshake = sasl.handleHandshake(new SaslHandshakeRequestData().setMechanism("PLAIN"));
        assertThat(handshake.errorCode()).isEqualTo(Errors.NONE.code());
        var authenticate = sasl.handleAuthenticate(new SaslAuthenticateRequestData()
                .setAuthBytes("\u0000alice\u0000secret".getBytes(StandardCharsets.UTF_8)));
        assertThat(authenticate).isInstanceOf(AuthenticationResult.Success.class);
        assertThat(manager.current()).isSameAs(config);
    }

    @Test
    void rejectedSnapshotLeavesPreviousStateIntact() {
        // given
        var virtualTopics = new VirtualTopicManager(Map.of());
        var authorizer = new RbacAuthorizer(new RbacConfig(Map.of(), Map.of()));
        var sasl = new SaslAuthenticator(Set.of());
        var manager = new DynamicConfigManager("localhost:9092", "__kawa", "test-group",
                virtualTopics, authorizer, sasl);
        var good = config(Map.of("orders", new VirtualTopicConfig("orders-v2")), rbacAllowingReadOnOrders(), plainAuth());
        manager.apply(good);
        var broken = config(
                Map.of("customers", new VirtualTopicConfig("crm.customers")),
                new RbacConfig(Map.of(), Map.of("team", new GroupConfig(List.of("alice"), List.of("missing-role")))),
                plainAuth());

        // when / then
        assertThatThrownBy(() -> manager.apply(broken))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing-role");
        assertThat(virtualTopics.toPhysical("orders")).isEqualTo("orders-v2");
        assertThat(virtualTopics.toPhysical("customers")).isEqualTo("customers");
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders", AclOperation.READ)).isTrue();
        assertThat(manager.current()).isSameAs(good);
    }

    @Test
    void lastConfigIsNullBeforeFirstSnapshot() {
        // given
        var manager = new DynamicConfigManager("localhost:9092", "__kawa", "test-group",
                new VirtualTopicManager(Map.of()),
                new RbacAuthorizer(new RbacConfig(Map.of(), Map.of())),
                new SaslAuthenticator(Set.of()));

        // when / then
        // An empty config topic on first boot is valid: no snapshot has been applied, so the
        // manager reports no current config and the consumers stay at their initial empty state.
        assertThat(manager.current()).isNull();
    }

    @Test
    void currentReturnsLastPersistedSnapshotBeforeConsumerAppliesIt() {
        // given - a manager whose write side is an in-memory repository (no broker needed)
        var writeRepository = new GatewayConfigRepository() {
            private GatewayConfig last;

            @Override
            public GatewayConfig current() {
                return last;
            }

            @Override
            public void write(GatewayConfig config) {
                last = config;
            }

            @Override
            public void close() {
                // no resources
            }
        };
        var manager = new DynamicConfigManager(writeRepository,
                new VirtualTopicManager(Map.of()),
                new RbacAuthorizer(new RbacConfig(Map.of(), Map.of())),
                new SaslAuthenticator(Set.of()));
        var first = config(Map.of("orders", new VirtualTopicConfig("orders-v2")), rbacAllowingReadOnOrders(), plainAuth());
        var second = config(Map.of("customers", new VirtualTopicConfig("crm.customers")), rbacAllowingReadOnOrders(), plainAuth());

        // when - two snapshots are persisted before the consumer has applied either
        manager.write(first);
        manager.write(second);

        // then - the read-modify-write base is the newest persisted snapshot, so a burst of
        // PUTs builds on each other instead of overwriting from the same stale base
        assertThat(manager.current()).isSameAs(second);
    }

    @Test
    void closeIsSafeWithoutStart() {
        // given
        var manager = new DynamicConfigManager("localhost:9092", "__kawa", "test-group",
                new VirtualTopicManager(Map.of()),
                new RbacAuthorizer(new RbacConfig(Map.of(), Map.of())),
                new SaslAuthenticator(Set.of()));

        // when / then
        manager.close();
    }
}
