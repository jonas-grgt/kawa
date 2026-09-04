package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

/// Gates FindCoordinator on the resource matching the request's `keyType`: GROUP lookups
/// (`keyType == 0`) on GROUP DESCRIBE, TRANSACTION lookups (`keyType == 1`) on TRANSACTIONAL_ID
/// DESCRIBE, both against the requested key. An unrecognized `keyType` is forwarded unchanged -
/// there is nothing to authorize against. This is an all-or-nothing gate like the
/// `WholeRequestAuthorizationCheck` family, but is a dedicated class rather than an instance of
/// that generic because the resource type is picked per-request off `keyType()` instead of being
/// fixed at construction time.
public final class FindCoordinatorAuthorizationCheck
        implements AuthorizationCheck<FindCoordinatorRequestData, FindCoordinatorResponseData> {

    private static final byte COORDINATOR_TYPE_GROUP = 0;
    private static final byte COORDINATOR_TYPE_TRANSACTION = 1;

    private final RbacAuthorizer authorizer;

    public FindCoordinatorAuthorizationCheck(RbacAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public short apiKey() {
        return ApiKeys.FIND_COORDINATOR.id;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, FindCoordinatorRequestData data) {
        String principal = context.principal();
        if (principal == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    new FindCoordinatorResponseData().setErrorCode(Errors.SASL_AUTHENTICATION_FAILED.code())));
            return;
        }
        if (data == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, new FindCoordinatorResponseData()));
            return;
        }
        Target target = switch (data.keyType()) {
            case COORDINATOR_TYPE_GROUP -> new Target(ResourceType.GROUP, Errors.GROUP_AUTHORIZATION_FAILED);
            case COORDINATOR_TYPE_TRANSACTION ->
                    new Target(ResourceType.TRANSACTIONAL_ID, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
            default -> null; // unrecognized keyType - nothing to authorize against, forward unchanged
        };
        if (target == null) {
            return;
        }
        if (!authorizer.isAuthorized(principal, target.resourceType, data.key(), AclOperation.DESCRIBE)) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    new FindCoordinatorResponseData().setErrorCode(target.denialError.code())));
        }
    }

    /// The resource type a `keyType` authorizes against, plus the error code returned when the
    /// principal is not authorized. Kept together so the two can't drift.
    private record Target(ResourceType resourceType, Errors denialError) {
    }
}
