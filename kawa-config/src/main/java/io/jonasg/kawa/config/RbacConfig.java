package io.jonasg.kawa.config;

import java.util.Map;

/// Role-based access control configuration: named roles carrying ACLs, and groups that
/// reference roles and list their members. A user's effective ACLs are the union of every
/// role referenced by every group they belong to.
///
/// @param roles named roles, each a list of ACLs
/// @param groups named groups, each a member list plus the roles those members inherit
public record RbacConfig(
        Map<String, RoleConfig> roles,
        Map<String, GroupConfig> groups
) {

    public RbacConfig {
        roles = roles == null ? Map.of() : Map.copyOf(roles);
        groups = groups == null ? Map.of() : Map.copyOf(groups);
    }
}
