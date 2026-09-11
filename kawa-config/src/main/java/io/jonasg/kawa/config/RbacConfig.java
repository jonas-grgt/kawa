package io.jonasg.kawa.config;

import java.util.HashMap;
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

    public RbacConfig() {
        this(Map.of(), Map.of());
    }

    public RbacConfig {
        roles = roles == null ? Map.of() : Map.copyOf(roles);
        groups = groups == null ? Map.of() : Map.copyOf(groups);
    }

    /// Returns a new [RbacConfig] with the given role added or replaced.
    public RbacConfig withRole(String name, RoleConfig role) {
        var newRoles = new HashMap<>(roles);
        newRoles.put(name, role);
        return new RbacConfig(newRoles, groups);
    }

    /// Returns a new [RbacConfig] with the given role removed.
    public RbacConfig withoutRole(String name) {
        var newRoles = new HashMap<>(roles);
        newRoles.remove(name);
        return new RbacConfig(newRoles, groups);
    }

    /// Returns a new [RbacConfig] with the given group added or replaced.
    public RbacConfig withGroup(String name, GroupConfig group) {
        var newGroups = new HashMap<>(groups);
        newGroups.put(name, group);
        return new RbacConfig(roles, newGroups);
    }

    /// Returns a new [RbacConfig] with the given group removed.
    public RbacConfig withoutGroup(String name) {
        var newGroups = new HashMap<>(groups);
        newGroups.remove(name);
        return new RbacConfig(roles, newGroups);
    }
}
