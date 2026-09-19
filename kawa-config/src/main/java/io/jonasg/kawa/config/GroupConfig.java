package io.jonasg.kawa.config;

import java.util.List;

/// A named group: the clients it contains and the roles those clients inherit.
///
/// @param clients the usernames in this group
/// @param roles   the roles whose ACLs every client inherits
public record GroupConfig(List<String> clients, List<String> roles) {

    public GroupConfig {
        clients = clients == null ? List.of() : List.copyOf(clients);
        roles = roles == null ? List.of() : List.copyOf(roles);
    }
}
