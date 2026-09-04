package io.jonasg.kawa.rbac;

import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.config.ResourceConfig;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/// Resolves the RBAC configuration into a per-user ACL list once at construction, then answers
/// authorization queries against that immutable map. Default-deny: a request is authorized only
/// if at least one matching ACL allows it, and any matching deny wins immediately.
public final class RbacAuthorizer {

    private final Map<String, List<AclConfig>> aclsByUser;

    public RbacAuthorizer(RbacConfig config) {
        Map<String, List<AclConfig>> byUser = new HashMap<>();
        config.groups().forEach((groupName, group) -> {
            List<AclConfig> groupAcls = group.roles().stream()
                    .flatMap(roleName -> {
                        var role = config.roles().get(roleName);
                        if (role == null) {
                            throw new IllegalArgumentException(
                                    "group '" + groupName + "' references unknown role '" + roleName + "'");
                        }
                        return role.acls().stream();
                    })
                    .toList();
            group.members().forEach(member ->
                    byUser.computeIfAbsent(member, k -> new ArrayList<>()).addAll(groupAcls));
        });
        this.aclsByUser = byUser.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        entry -> List.copyOf(entry.getValue())));
    }

    /// Whether `principal` is authorized to perform `operation` on `resource` of `type`.
    ///
    /// Default-deny: `false` unless at least one matching ACL allows it. Any matching
    /// [AclPermissionType#DENY] ACL denies immediately, regardless of what else matches.
    public boolean isAuthorized(String principal, ResourceType type, String resource, AclOperation operation) {
        boolean allowed = false;
        for (AclConfig acl : aclsFor(principal)) {
            if (!matches(acl, type, resource, operation)) {
                continue;
            }
            if (acl.permission() == AclPermissionType.DENY) {
                return false; // deny wins, stop immediately
            }
            allowed = true;
        }
        return allowed; // false if nothing matched at all - default deny
    }

    private List<AclConfig> aclsFor(String principal) {
        return aclsByUser.getOrDefault(principal, List.of());
    }

    private static boolean matches(AclConfig acl, ResourceType type, String resource, AclOperation operation) {
        ResourceConfig res = acl.resource();
        if (res.type() != type) {
            return false;
        }
        if (operation != acl.operation() && acl.operation() != AclOperation.ALL) {
            return false;
        }
        if (res.type() == ResourceType.CLUSTER) {
            return true; // pattern is null; matches the cluster regardless of name
        }
        String pattern = res.pattern();
        if (res.patternType() == PatternType.PREFIXED) {
            return resource.startsWith(pattern);
        }
        return resource.equals(pattern); // LITERAL exact match
    }
}
