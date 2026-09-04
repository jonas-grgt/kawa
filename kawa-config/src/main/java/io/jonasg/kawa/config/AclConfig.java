package io.jonasg.kawa.config;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;

/// A single access-control entry: a resource, an operation, and whether access is granted
/// or denied. Permission defaults to [AclPermissionType#ALLOW] when omitted.
///
/// @param resource the resource this ACL applies to
/// @param operation the operation being granted or denied
/// @param permission whether access is allowed or denied
public record AclConfig(
        ResourceConfig resource,
        AclOperation operation,
        AclPermissionType permission
) {

    /// Convenience constructor for an [AclPermissionType#ALLOW] ACL - the common case, matching
    /// the YAML default where `permission` is omitted.
    public AclConfig(ResourceConfig resource, AclOperation operation) {
        this(resource, operation, AclPermissionType.ALLOW);
    }

    public AclConfig {
        if (resource == null) {
            throw new IllegalArgumentException("resource must not be null");
        }
        if (operation == null) {
            throw new IllegalArgumentException("operation must not be null");
        }
        if (permission == null) {
            permission = AclPermissionType.ALLOW;
        }
    }
}
