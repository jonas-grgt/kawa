package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/// Gates TxnOffsetCommit on three resources: a whole-request TRANSACTIONAL_ID WRITE check on the
/// request's transactionalId, a whole-request GROUP READ check on the request's groupId, and a
/// per-topic TOPIC READ check on each topic. If either whole-request gate fails (or there is no
/// authenticated principal) the whole request is short-circuited with an AUTHORIZATION_FAILED (or
/// SASL_AUTHENTICATION_FAILED) response for every topic/partition - nothing can proceed without
/// transactional-id and consumer-group access, so the per-topic checks are skipped entirely.
/// Otherwise authorized topics are forwarded, denied topics are stripped from the request and
/// remembered so the response can be reconstructed with a TOPIC_AUTHORIZATION_FAILED partition
/// response per denied partition.
public final class TxnOffsetCommitAuthorizationCheck implements AuthorizationCheck<TxnOffsetCommitRequestData, TxnOffsetCommitResponseData> {

    private final RbacAuthorizer authorizer;

    public TxnOffsetCommitAuthorizationCheck(RbacAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public short apiKey() {
        return ApiKeys.TXN_OFFSET_COMMIT.id;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, TxnOffsetCommitRequestData data) {
        String principal = context.principal();
        if (principal == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    denialResponse(data, Errors.SASL_AUTHENTICATION_FAILED)));
            return;
        }
        if (data == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, new TxnOffsetCommitResponseData()));
            return;
        }
        // Gate 1: TRANSACTIONAL_ID WRITE. All-or-nothing.
        if (!authorizer.isAuthorized(principal, ResourceType.TRANSACTIONAL_ID,
                data.transactionalId(), AclOperation.WRITE)) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    denialResponse(data, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)));
            return;
        }
        // Gate 2: GROUP READ. All-or-nothing.
        if (!authorizer.isAuthorized(principal, ResourceType.GROUP, data.groupId(), AclOperation.READ)) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    denialResponse(data, Errors.GROUP_AUTHORIZATION_FAILED)));
            return;
        }
        // Gate 3: per-topic TOPIC READ.
        TxnOffsetCommitAuthState state = null;
        var denied = new ArrayList<TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic>();
        for (TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic topic : data.topics()) {
            if (!authorizer.isAuthorized(principal, ResourceType.TOPIC, topic.name(), AclOperation.READ)) {
                denied.add(topic);
            }
        }
        if (!denied.isEmpty()) {
            Set<String> deniedNames = new HashSet<>();
            for (TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic topic : denied) {
                deniedNames.add(topic.name());
            }
            data.topics().removeIf(topic -> deniedNames.contains(topic.name()));
            state = context.state(TxnOffsetCommitAuthState.class);
            if (state == null) {
                state = new TxnOffsetCommitAuthState();
                context.state(TxnOffsetCommitAuthState.class, state);
            }
            for (TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic topic : denied) {
                state.recordDenied(topic.name(), partitionIndices(topic));
            }
        }
        if (state != null && data.topics().isEmpty()) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    topicDenialResponse(state.deniedPartitionsByTopic(), Errors.TOPIC_AUTHORIZATION_FAILED)));
        }
    }

    @Override
    public void onResponse(GatewayContext context, TxnOffsetCommitResponseData data) {
        TxnOffsetCommitAuthState state = context.state(TxnOffsetCommitAuthState.class);
        if (state != null && !state.deniedPartitionsByTopic().isEmpty()) {
            mergeDenials(data, state);
        }
    }

    private static void mergeDenials(TxnOffsetCommitResponseData data, TxnOffsetCommitAuthState state) {
        for (Map.Entry<String, List<Integer>> entry : state.deniedPartitionsByTopic().entrySet()) {
            data.topics().add(topicDenialResponse(entry.getKey(), entry.getValue(), Errors.TOPIC_AUTHORIZATION_FAILED));
        }
    }

    private static TxnOffsetCommitResponseData denialResponse(TxnOffsetCommitRequestData data, Errors error) {
        var response = new TxnOffsetCommitResponseData();
        if (data != null) {
            for (TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic topic : data.topics()) {
                response.topics().add(topicDenialResponse(topic.name(), partitionIndices(topic), error));
            }
        }
        return response;
    }

    private static TxnOffsetCommitResponseData topicDenialResponse(Map<String, List<Integer>> deniedByTopic, Errors error) {
        var response = new TxnOffsetCommitResponseData();
        for (Map.Entry<String, List<Integer>> entry : deniedByTopic.entrySet()) {
            response.topics().add(topicDenialResponse(entry.getKey(), entry.getValue(), error));
        }
        return response;
    }

    private static TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic topicDenialResponse(
            String topic,
            List<Integer> partitions,
            Errors error
    ) {
        var topicResponse = new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic().setName(topic);
        for (int index : partitions) {
            topicResponse.partitions().add(new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                    .setPartitionIndex(index)
                    .setErrorCode(error.code()));
        }
        return topicResponse;
    }

    private static List<Integer> partitionIndices(TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic topic) {
        return topic.partitions().stream()
                .map(TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition::partitionIndex)
                .toList();
    }
}
