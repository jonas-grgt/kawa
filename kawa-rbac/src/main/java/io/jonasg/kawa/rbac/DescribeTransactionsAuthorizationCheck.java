package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.List;

/// Gates DescribeTransactions per transaction id on TRANSACTIONAL_ID DESCRIBE. A request can name
/// several ids, so this strips denied ids from the request (remembering them) and merges a
/// TRANSACTIONAL_ID_AUTHORIZATION_FAILED entry per denied id into the response. A request without
/// an authenticated principal is denied rather than passed through, so skipping SASL cannot bypass
/// RBAC.
public final class DescribeTransactionsAuthorizationCheck implements AuthorizationCheck<DescribeTransactionsRequestData, DescribeTransactionsResponseData> {

    private final RbacAuthorizer authorizer;

    public DescribeTransactionsAuthorizationCheck(RbacAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public short apiKey() {
        return ApiKeys.DESCRIBE_TRANSACTIONS.id;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, DescribeTransactionsRequestData data) {
        String principal = context.principal();
        if (principal == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    describeTransactionsDenialResponse(data == null ? List.of() : data.transactionalIds(),
                            Errors.SASL_AUTHENTICATION_FAILED)));
            return;
        }
        if (data == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, new DescribeTransactionsResponseData()));
            return;
        }
        DescribeTransactionsAuthState state = null;
        var denied = new ArrayList<String>();
        for (String transactionalId : data.transactionalIds()) {
            if (!authorizer.isAuthorized(principal, ResourceType.TRANSACTIONAL_ID, transactionalId, AclOperation.DESCRIBE)) {
                denied.add(transactionalId);
            }
        }
        data.transactionalIds().removeAll(denied);
        if (!denied.isEmpty()) {
            state = new DescribeTransactionsAuthState();
            state.recordDenied(denied);
            context.state(DescribeTransactionsAuthState.class, state);
        }
        if (state != null && data.transactionalIds().isEmpty()) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    describeTransactionsDenialResponse(state.deniedTransactionalIds(), Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)));
        }
    }

    @Override
    public void onResponse(GatewayContext context, DescribeTransactionsResponseData data) {
        DescribeTransactionsAuthState state = context.state(DescribeTransactionsAuthState.class);
        if (state == null || state.deniedTransactionalIds().isEmpty()) {
            return;
        }
        for (String transactionalId : state.deniedTransactionalIds()) {
            data.transactionStates().add(new DescribeTransactionsResponseData.TransactionState()
                    .setTransactionalId(transactionalId)
                    .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code()));
        }
    }

    private static DescribeTransactionsResponseData describeTransactionsDenialResponse(List<String> transactionalIds, Errors error) {
        var response = new DescribeTransactionsResponseData();
        for (String transactionalId : transactionalIds) {
            response.transactionStates().add(new DescribeTransactionsResponseData.TransactionState()
                    .setTransactionalId(transactionalId)
                    .setErrorCode(error.code()));
        }
        return response;
    }
}
