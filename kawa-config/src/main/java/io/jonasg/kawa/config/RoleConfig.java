package io.jonasg.kawa.config;

import java.util.List;

/// A named role: the list of ACLs it grants or denies.
///
/// @param acls the ACLs making up this role
public record RoleConfig(List<AclConfig> acls) {

    public RoleConfig {
        acls = acls == null ? List.of() : List.copyOf(acls);
    }
}
