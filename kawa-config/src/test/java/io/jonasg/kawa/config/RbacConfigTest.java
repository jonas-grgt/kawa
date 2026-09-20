package io.jonasg.kawa.config;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RbacConfigTest {

    @Test
    void nullRolesAndGroupsCoalesceToEmpty() {
        // when
        RbacConfig config = new RbacConfig(null, null);

        // then
        assertThat(config.roles()).isEmpty();
        assertThat(config.groups()).isEmpty();
    }

    @Test
    void nullAclsCoalesceToEmptyList() {
        // when
        RoleConfig role = new RoleConfig(null);

        // then
        assertThat(role.acls()).isEmpty();
    }

    @Test
    void nullClientsAndRolesCoalesceToEmptyLists() {
        // when
        GroupConfig group = new GroupConfig(null, null);

        // then
        assertThat(group.clients()).isEmpty();
        assertThat(group.roles()).isEmpty();
    }

    @Test
    void permissionDefaultsToAllowWhenOmitted() {
        // when
        AclConfig acl = new AclConfig(
                new ResourceConfig(ResourceType.TOPIC, "orders", null),
                AclOperation.READ,
                null);

        // then
        assertThat(acl.permission()).isEqualTo(AclPermissionType.ALLOW);
    }

    @Test
    void patternTypeDefaultsToLiteralWhenOmitted() {
        // when
        ResourceConfig resource = new ResourceConfig(ResourceType.TOPIC, "orders", null);

        // then
        assertThat(resource.patternType()).isEqualTo(PatternType.LITERAL);
    }

    @Test
    void topicRequiresAPattern() {
        // when / then
        assertThatThrownBy(() -> new ResourceConfig(ResourceType.TOPIC, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pattern");
    }

    @Test
    void groupRequiresAPattern() {
        // when / then
        assertThatThrownBy(() -> new ResourceConfig(ResourceType.GROUP, " ", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pattern");
    }

    @Test
    void prefixedResourceAllowsBlankPatternAsAnyResourceWildcard() {
        // when
        ResourceConfig topic = new ResourceConfig(ResourceType.TOPIC, "", PatternType.PREFIXED);
        ResourceConfig group = new ResourceConfig(ResourceType.GROUP, "", PatternType.PREFIXED);

        // then
        assertThat(topic.pattern()).isEmpty();
        assertThat(group.pattern()).isEmpty();
    }

    @Test
    void literalResourceStillRejectsBlankPattern() {
        // when / then
        assertThatThrownBy(() -> new ResourceConfig(ResourceType.TOPIC, "", PatternType.LITERAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pattern");
    }

    @Test
    void clusterAllowsNullPatternAndForcesItToNull() {
        // when
        ResourceConfig resource = new ResourceConfig(ResourceType.CLUSTER, "ignored", null);

        // then
        assertThat(resource.pattern()).isNull();
    }

    @Test
    void aclRequiresResourceAndOperation() {
        // when / then
        assertThatThrownBy(() -> new AclConfig(null, AclOperation.READ, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("resource");
        assertThatThrownBy(() -> new AclConfig(
                new ResourceConfig(ResourceType.TOPIC, "orders", null), null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("operation");
    }

    @Test
    void copiesAreImmutable() {
        // given
        List<AclConfig> acls = new java.util.ArrayList<>(List.of(
                new AclConfig(new ResourceConfig(ResourceType.TOPIC, "orders", null),
                        AclOperation.READ, AclPermissionType.ALLOW)));
        Map<String, RoleConfig> roles = new java.util.HashMap<>(Map.of("reader", new RoleConfig(acls)));
        Map<String, GroupConfig> groups = new java.util.HashMap<>(Map.of(
                "team", new GroupConfig(new java.util.ArrayList<>(List.of("alice")),
                        new java.util.ArrayList<>(List.of("reader")))));

        // when
        RbacConfig config = new RbacConfig(roles, groups);

        // then
        assertThatThrownBy(() -> config.roles().put("x", null))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> config.groups().put("x", null))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> config.roles().get("reader").acls().add(null))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> config.groups().get("team").clients().add("bob"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void upsertRoleAddsNewRole() {
        // given
        var config = new RbacConfig(null, null);

        // when
        var updated = config.upsertRole("admin", new RoleConfig(List.of()));

        // then
        assertThat(updated.roles()).containsEntry("admin", new RoleConfig(List.of()));
    }

    @Test
    void upsertRoleOverwritesExisting() {
        // given
        var config = new RbacConfig(Map.of("admin", new RoleConfig(List.of())), null);
        var newRole = new RoleConfig(List.of(
                new AclConfig(new ResourceConfig(ResourceType.TOPIC, "orders", null),
                        AclOperation.READ, AclPermissionType.ALLOW)));

        // when
        var updated = config.upsertRole("admin", newRole);

        // then
        assertThat(updated.roles()).containsEntry("admin", newRole);
        assertThat(updated.roles()).hasSize(1);
    }

    @Test
    void removeRoleRemovesExisting() {
        // given
        var config = new RbacConfig(Map.of("admin", new RoleConfig(List.of())), null);

        // when
        var updated = config.removeRole("admin");

        // then
        assertThat(updated.roles()).isEmpty();
    }

    @Test
    void removeRolePreservesOtherRoles() {
        // given
        var config = new RbacConfig(Map.of(
                "admin", new RoleConfig(List.of()),
                "reader", new RoleConfig(List.of())), null);

        // when
        var updated = config.removeRole("admin");

        // then
        assertThat(updated.roles()).hasSize(1);
        assertThat(updated.roles()).containsKey("reader");
    }

    @Test
    void removeRoleRemovesRoleFromReferencingGroup() {
        // given
        var config = new RbacConfig(
                Map.of("reader", new RoleConfig(List.of())),
                Map.of("team-a", new GroupConfig(List.of("alice"), List.of("reader", "writer"))));

        // when
        var updated = config.removeRole("reader");

        // then
        assertThat(updated.roles()).isEmpty();
        assertThat(updated.groups().get("team-a").roles())
                .containsExactly("writer");
    }

    @Test
    void removeRoleLeavesGroupsWithoutTheRoleUntouched() {
        // given
        var config = new RbacConfig(
                Map.of("reader", new RoleConfig(List.of()), "admin", new RoleConfig(List.of())),
                Map.of("team-a", new GroupConfig(List.of("alice"), List.of("admin"))));

        // when
        var updated = config.removeRole("reader");

        // then
        assertThat(updated.groups().get("team-a").roles()).containsExactly("admin");
    }

    @Test
    void removeRoleRemovesAllOccurrencesFromAGroup() {
        // given
        var config = new RbacConfig(
                Map.of("reader", new RoleConfig(List.of())),
                Map.of("team-a", new GroupConfig(List.of("alice"), List.of("reader", "reader"))));

        // when
        var updated = config.removeRole("reader");

        // then
        assertThat(updated.groups().get("team-a").roles()).isEmpty();
    }

    @Test
    void removeRolePreservesGroupClients() {
        // given
        var config = new RbacConfig(
                Map.of("reader", new RoleConfig(List.of())),
                Map.of("team-a", new GroupConfig(List.of("alice", "bob"), List.of("reader"))));

        // when
        var updated = config.removeRole("reader");

        // then
        assertThat(updated.groups().get("team-a").clients()).containsExactly("alice", "bob");
    }

    @Test
    void upsertGroupAddsNewGroup() {
        // given
        var config = new RbacConfig(null, null);

        // when
        var updated = config.upsertGroup("team-a", new GroupConfig(List.of("alice"), List.of("reader")));

        // then
        assertThat(updated.groups()).containsEntry("team-a", new GroupConfig(List.of("alice"), List.of("reader")));
    }

    @Test
    void upsertGroupOverwritesExisting() {
        // given
        var config = new RbacConfig(null, Map.of("team-a", new GroupConfig(List.of("bob"), List.of())));
        var newGroup = new GroupConfig(List.of("alice"), List.of("admin"));

        // when
        var updated = config.upsertGroup("team-a", newGroup);

        // then
        assertThat(updated.groups()).containsEntry("team-a", newGroup);
        assertThat(updated.groups()).hasSize(1);
    }

    @Test
    void removeGroupRemovesExisting() {
        // given
        var config = new RbacConfig(null, Map.of("team-a", new GroupConfig(List.of("alice"), List.of())));

        // when
        var updated = config.removeGroup("team-a");

        // then
        assertThat(updated.groups()).isEmpty();
    }

    @Test
    void removeGroupPreservesOtherGroups() {
        // given
        var config = new RbacConfig(null, Map.of(
                "team-a", new GroupConfig(List.of("alice"), List.of()),
                "team-b", new GroupConfig(List.of("bob"), List.of())));

        // when
        var updated = config.removeGroup("team-a");

        // then
        assertThat(updated.groups()).hasSize(1);
        assertThat(updated.groups()).containsKey("team-b");
    }

    @Test
    void upsertRolePreservesGroups() {
        // given
        var config = new RbacConfig(
                Map.of("reader", new RoleConfig(List.of())),
                Map.of("team-a", new GroupConfig(List.of("alice"), List.of("reader"))));

        // when
        var updated = config.upsertRole("admin", new RoleConfig(List.of()));

        // then
        assertThat(updated.roles()).hasSize(2);
        assertThat(updated.groups()).containsEntry("team-a",
                new GroupConfig(List.of("alice"), List.of("reader")));
    }

    @Test
    void renameGroupReKeysGroupPreservingClientsAndRoles() {
        // given
        var config = new RbacConfig(
                Map.of("reader", new RoleConfig(List.of())),
                Map.of("team-a", new GroupConfig(List.of("alice"), List.of("reader"))));

        // when
        var updated = config.renameGroup("team-a", "team-b");

        // then
        assertThat(updated.groups()).doesNotContainKey("team-a");
        assertThat(updated.groups().get("team-b"))
                .isEqualTo(new GroupConfig(List.of("alice"), List.of("reader")));
    }

    @Test
    void renameGroupPreservesOtherGroups() {
        // given
        var config = new RbacConfig(
                null,
                Map.of(
                        "team-a", new GroupConfig(List.of("alice"), List.of()),
                        "team-b", new GroupConfig(List.of("bob"), List.of())));

        // when
        var updated = config.renameGroup("team-a", "team-c");

        // then
        assertThat(updated.groups()).hasSize(2);
        assertThat(updated.groups()).containsKey("team-b");
        assertThat(updated.groups()).containsKey("team-c");
    }

    @Test
    void renameGroupMissingSourceIsNoOp() {
        // given
        var config = new RbacConfig(null, Map.of("team-a", new GroupConfig(List.of("alice"), List.of())));

        // when
        var updated = config.renameGroup("missing", "team-b");

        // then
        assertThat(updated.groups()).containsOnlyKeys("team-a");
    }
}
