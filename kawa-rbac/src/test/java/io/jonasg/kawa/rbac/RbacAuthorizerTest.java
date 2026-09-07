package io.jonasg.kawa.rbac;

import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.config.ResourceConfig;
import io.jonasg.kawa.config.RoleConfig;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RbacAuthorizerTest {

    private static AclConfig acl(ResourceType type, String pattern, PatternType patternType,
                                 AclOperation operation, AclPermissionType permission) {
        return new AclConfig(new ResourceConfig(type, pattern, patternType), operation, permission);
    }

    private static RbacAuthorizer authorizer(RoleConfig... roles) {
        Map<String, RoleConfig> roleMap = new java.util.HashMap<>();
        List<String> roleNames = new java.util.ArrayList<>();
        for (int i = 0; i < roles.length; i++) {
            String name = "role" + i;
            roleMap.put(name, roles[i]);
            roleNames.add(name);
        }
        return new RbacAuthorizer(new RbacConfig(roleMap, Map.of(
                "team", new GroupConfig(List.of("alice"), roleNames))));
    }

    @Test
    void deniesByDefaultWhenNothingMatches() {
        // given
        var authorizer = authorizer(new RoleConfig(List.of(
                acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.READ, AclPermissionType.ALLOW))));

        // when / then
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "other", AclOperation.READ)).isFalse();
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders", AclOperation.WRITE)).isFalse();
        assertThat(authorizer.isAuthorized("bob", ResourceType.TOPIC, "orders", AclOperation.READ)).isFalse();
    }

    @Test
    void allowsOnExactLiteralMatch() {
        // given
        var authorizer = authorizer(new RoleConfig(List.of(
                acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.READ, AclPermissionType.ALLOW))));

        // when / then
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders", AclOperation.READ)).isTrue();
    }

    @Test
    void allowsOnPrefixedMatch() {
        // given
        var authorizer = authorizer(new RoleConfig(List.of(
                acl(ResourceType.TOPIC, "orders.", PatternType.PREFIXED, AclOperation.READ, AclPermissionType.ALLOW))));

        // when / then
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders.eu", AclOperation.READ)).isTrue();
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders", AclOperation.READ)).isFalse();
    }

    @Test
    void emptyPrefixedPatternMatchesAnyResourceOfThatType() {
        // given
        var authorizer = authorizer(new RoleConfig(List.of(
                acl(ResourceType.TOPIC, "", PatternType.PREFIXED, AclOperation.ALL, AclPermissionType.ALLOW),
                acl(ResourceType.GROUP, "", PatternType.PREFIXED, AclOperation.ALL, AclPermissionType.ALLOW))));

        // when / then
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "any-topic", AclOperation.WRITE)).isTrue();
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "another", AclOperation.READ)).isTrue();
        assertThat(authorizer.isAuthorized("alice", ResourceType.GROUP, "any-group", AclOperation.READ)).isTrue();
    }

    @Test
    void allOperationMatchesAnyOperation() {
        // given
        var authorizer = authorizer(new RoleConfig(List.of(
                acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.ALL, AclPermissionType.ALLOW))));

        // when / then
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders", AclOperation.READ)).isTrue();
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders", AclOperation.WRITE)).isTrue();
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders", AclOperation.DELETE)).isTrue();
    }

    @Test
    void denyWinsOverAllow() {
        // given
        var authorizer = authorizer(
                new RoleConfig(List.of(
                        acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.READ, AclPermissionType.ALLOW))),
                new RoleConfig(List.of(
                        acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.READ, AclPermissionType.DENY))));

        // when / then
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders", AclOperation.READ)).isFalse();
    }

    @Test
    void allOperationDenyDeniesEveryOperation() {
        // given
        var authorizer = authorizer(new RoleConfig(List.of(
                acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.ALL, AclPermissionType.DENY))));

        // when / then
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders", AclOperation.READ)).isFalse();
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders", AclOperation.WRITE)).isFalse();
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders", AclOperation.DELETE)).isFalse();
    }

    @Test
    void clusterMatchesRegardlessOfResourceName() {
        // given
        var authorizer = authorizer(new RoleConfig(List.of(
                acl(ResourceType.CLUSTER, null, PatternType.LITERAL, AclOperation.CREATE, AclPermissionType.ALLOW))));

        // when / then
        assertThat(authorizer.isAuthorized("alice", ResourceType.CLUSTER, "kafka-cluster", AclOperation.CREATE)).isTrue();
        assertThat(authorizer.isAuthorized("alice", ResourceType.CLUSTER, "anything", AclOperation.CREATE)).isTrue();
    }

    @Test
    void userInMultipleGroupsGetsUnionOfRoles() {
        // given
        var reader = new RoleConfig(List.of(
                acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.READ, AclPermissionType.ALLOW)));
        var writer = new RoleConfig(List.of(
                acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.WRITE, AclPermissionType.ALLOW)));
        var authorizer = new RbacAuthorizer(new RbacConfig(
                Map.of("reader", reader, "writer", writer),
                Map.of(
                        "readers", new GroupConfig(List.of("alice"), List.of("reader")),
                        "writers", new GroupConfig(List.of("alice"), List.of("writer")))));

        // when / then
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders", AclOperation.READ)).isTrue();
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders", AclOperation.WRITE)).isTrue();
    }

    @Test
    void unknownRoleReferenceFailsAtConstruction() {
        // given
        var config = new RbacConfig(Map.of(), Map.of(
                "team", new GroupConfig(List.of("alice"), List.of("missing-role"))));

        // when / then
        assertThatThrownBy(() -> new RbacAuthorizer(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing-role");
    }

    @Test
    void reloadReplacesAcls() {
        // given
        var authorizer = authorizer(new RoleConfig(List.of(
                acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.READ, AclPermissionType.ALLOW))));
        var replacement = new RbacConfig(
                Map.of("writer", new RoleConfig(List.of(
                        acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.WRITE, AclPermissionType.ALLOW)))),
                Map.of("team", new GroupConfig(List.of("alice"), List.of("writer"))));

        // when
        authorizer.reload(replacement);

        // then
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders", AclOperation.READ)).isFalse();
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders", AclOperation.WRITE)).isTrue();
    }

    @Test
    void reloadWithEmptyConfigDeniesEverything() {
        // given
        var authorizer = authorizer(new RoleConfig(List.of(
                acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.READ, AclPermissionType.ALLOW))));

        // when
        authorizer.reload(new RbacConfig(Map.of(), Map.of()));

        // then
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders", AclOperation.READ)).isFalse();
    }

    @Test
    void failedReloadKeepsPreviousAcls() {
        // given
        var authorizer = authorizer(new RoleConfig(List.of(
                acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.READ, AclPermissionType.ALLOW))));
        var broken = new RbacConfig(Map.of(), Map.of(
                "team", new GroupConfig(List.of("alice"), List.of("missing-role"))));

        // when / then
        assertThatThrownBy(() -> authorizer.reload(broken))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing-role");
        assertThat(authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders", AclOperation.READ)).isTrue();
    }

    @Test
    void reloadIsSafeDuringConcurrentReads() throws Exception {
        // given
        var authorizer = authorizer(new RoleConfig(List.of(
                acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.READ, AclPermissionType.ALLOW))));
        var first = new RbacConfig(
                Map.of("reader", new RoleConfig(List.of(
                        acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.READ, AclPermissionType.ALLOW)))),
                Map.of("team", new GroupConfig(List.of("alice"), List.of("reader"))));
        var second = new RbacConfig(
                Map.of("writer", new RoleConfig(List.of(
                        acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.WRITE, AclPermissionType.ALLOW)))),
                Map.of("team", new GroupConfig(List.of("alice"), List.of("writer"))));
        var failure = new AtomicReference<Throwable>();

        // when
        var writer = new Thread(() -> {
            for (int i = 0; i < 10_000; i++) {
                authorizer.reload(i % 2 == 0 ? first : second);
            }
        });
        var reader = new Thread(() -> {
            try {
                for (int i = 0; i < 10_000; i++) {
                    authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders", AclOperation.READ);
                    authorizer.isAuthorized("alice", ResourceType.TOPIC, "orders", AclOperation.WRITE);
                    authorizer.isAuthorized("bob", ResourceType.TOPIC, "orders", AclOperation.READ);
                }
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        writer.start();
        reader.start();
        writer.join();
        reader.join();

        // then
        assertThat(failure).hasValue(null);
    }
}
