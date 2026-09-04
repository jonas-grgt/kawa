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
    void nullMembersAndRolesCoalesceToEmptyLists() {
        // when
        GroupConfig group = new GroupConfig(null, null);

        // then
        assertThat(group.members()).isEmpty();
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
        assertThatThrownBy(() -> config.groups().get("team").members().add("bob"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
