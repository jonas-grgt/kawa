package io.jonasg.kawa.http;

import java.util.List;

/// A single RBAC group in the admin `/rbac/groups` response.
///
/// @param name the group name
/// @param members the usernames in this group
/// @param roles the roles whose ACLs every member inherits
public record GroupView(
        String name,
        List<String> members,
        List<String> roles) {
}