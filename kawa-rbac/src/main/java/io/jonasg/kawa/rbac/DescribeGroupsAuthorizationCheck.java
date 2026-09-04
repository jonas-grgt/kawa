package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.List;

/// Gates DescribeGroups per group on GROUP DESCRIBE. A request can name several groups, so this
/// strips denied groups from the request (remembering them) and merges a
/// GROUP_AUTHORIZATION_FAILED entry per denied group into the response. Denial uses
/// GROUP_AUTHORIZATION_FAILED (not the anti-enumeration UNKNOWN-style code Metadata uses) -
/// groups don't carry the same existence-hiding concern real Kafka gives topics, and this stays
/// consistent with the GROUP_AUTHORIZATION_FAILED already used for JoinGroup/SyncGroup/Heartbeat/
/// LeaveGroup. A request without an authenticated principal is denied rather than passed through.
public final class DescribeGroupsAuthorizationCheck implements AuthorizationCheck<DescribeGroupsRequestData, DescribeGroupsResponseData> {

    private final RbacAuthorizer authorizer;

    public DescribeGroupsAuthorizationCheck(RbacAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public short apiKey() {
        return ApiKeys.DESCRIBE_GROUPS.id;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, DescribeGroupsRequestData data) {
        String principal = context.principal();
        if (principal == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    describeGroupsDenialResponse(data == null ? List.of() : data.groups(), Errors.SASL_AUTHENTICATION_FAILED)));
            return;
        }
        if (data == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, new DescribeGroupsResponseData()));
            return;
        }
        DescribeGroupsAuthState state = null;
        var denied = new ArrayList<String>();
        for (String groupId : data.groups()) {
            if (!authorizer.isAuthorized(principal, ResourceType.GROUP, groupId, AclOperation.DESCRIBE)) {
                denied.add(groupId);
            }
        }
        data.groups().removeAll(denied);
        if (!denied.isEmpty()) {
            state = new DescribeGroupsAuthState();
            state.recordDenied(denied);
            context.state(DescribeGroupsAuthState.class, state);
        }
        if (state != null && data.groups().isEmpty()) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    describeGroupsDenialResponse(state.deniedGroups(), Errors.GROUP_AUTHORIZATION_FAILED)));
        }
    }

    @Override
    public void onResponse(GatewayContext context, DescribeGroupsResponseData data) {
        DescribeGroupsAuthState state = context.state(DescribeGroupsAuthState.class);
        if (state == null || state.deniedGroups().isEmpty()) {
            return;
        }
        for (String groupId : state.deniedGroups()) {
            data.groups().add(new DescribeGroupsResponseData.DescribedGroup()
                    .setGroupId(groupId)
                    .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code()));
        }
    }

    private static DescribeGroupsResponseData describeGroupsDenialResponse(List<String> groupIds, Errors error) {
        var response = new DescribeGroupsResponseData();
        for (String groupId : groupIds) {
            response.groups().add(new DescribeGroupsResponseData.DescribedGroup()
                    .setGroupId(groupId)
                    .setErrorCode(error.code()));
        }
        return response;
    }
}
