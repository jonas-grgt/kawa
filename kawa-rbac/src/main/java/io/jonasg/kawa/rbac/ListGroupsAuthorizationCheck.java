package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

/// Gates ListGroups visibility on GROUP DESCRIBE. The request names no specific groups, so for an
/// authenticated principal there's nothing to check before forwarding - the response is filtered
/// instead. A request without an authenticated principal is denied with
/// SASL_AUTHENTICATION_FAILED rather than passed through, so skipping SASL cannot bypass RBAC.
public final class ListGroupsAuthorizationCheck implements AuthorizationCheck<ListGroupsRequestData, ListGroupsResponseData> {

    private final RbacAuthorizer authorizer;

    public ListGroupsAuthorizationCheck(RbacAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public short apiKey() {
        return ApiKeys.LIST_GROUPS.id;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, ListGroupsRequestData body) {
        if (context.principal() == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    new ListGroupsResponseData().setErrorCode(Errors.SASL_AUTHENTICATION_FAILED.code())));
        }
    }

    @Override
    public void onResponse(GatewayContext context, ListGroupsResponseData data) {
        String principal = context.principal();
        if (principal == null) {
            // Defence-in-depth: onRequest already short-circuits unauthenticated requests, so this
            // is only reachable if a short-circuit was bypassed.
            data.groups().clear();
            return;
        }
        data.groups().removeIf(group ->
                !authorizer.isAuthorized(principal, ResourceType.GROUP, group.groupId(), AclOperation.DESCRIBE));
    }
}
