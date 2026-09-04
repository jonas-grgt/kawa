package io.jonasg.kawa.config;

import java.util.List;

/// A named group: the members it contains and the roles those members inherit.
///
/// @param members the usernames in this group
/// @param roles the roles whose ACLs every member inherits
public record GroupConfig(List<String> members, List<String> roles) {

    public GroupConfig {
        members = members == null ? List.of() : List.copyOf(members);
        roles = roles == null ? List.of() : List.copyOf(roles);
    }
}
