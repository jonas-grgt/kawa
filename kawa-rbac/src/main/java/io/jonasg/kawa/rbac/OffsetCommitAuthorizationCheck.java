package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Gates OffsetCommit on two resources: a whole-request GROUP READ check on the request's
/// groupId, and a per-topic TOPIC READ check on each topic. If the group gate fails (or there
/// is no authenticated principal) the whole request is short-circuited with a
/// GROUP_AUTHORIZATION_FAILED (or SASL_AUTHENTICATION_FAILED) response for every topic/partition
/// - nothing can proceed without group access, so the per-topic checks are skipped entirely.
/// Otherwise authorized topics are forwarded, denied topics are stripped from the request and
/// remembered so the response can be reconstructed with a TOPIC_AUTHORIZATION_FAILED partition
/// response per denied partition.
public final class OffsetCommitAuthorizationCheck implements AuthorizationCheck<OffsetCommitRequestData, OffsetCommitResponseData> {

    private final RbacAuthorizer authorizer;

    public OffsetCommitAuthorizationCheck(RbacAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public short apiKey() {
        return ApiKeys.OFFSET_COMMIT.id;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, OffsetCommitRequestData data) {
        String principal = context.principal();
        if (principal == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    groupDenialResponse(data, Errors.SASL_AUTHENTICATION_FAILED)));
            return;
        }
        if (data == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, new OffsetCommitResponseData()));
            return;
        }
        // Gate 1: GROUP READ on the groupId. All-or-nothing - if denied, nothing can proceed.
        if (!authorizer.isAuthorized(principal, ResourceType.GROUP, data.groupId(), AclOperation.READ)) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    groupDenialResponse(data, Errors.GROUP_AUTHORIZATION_FAILED)));
            return;
        }
        // Gate 2: per-topic TOPIC READ.
        OffsetCommitAuthState state = null;
        var denied = new ArrayList<OffsetCommitRequestData.OffsetCommitRequestTopic>();
        for (OffsetCommitRequestData.OffsetCommitRequestTopic topic : data.topics()) {
            if (!authorizer.isAuthorized(principal, ResourceType.TOPIC, topic.name(), AclOperation.READ)) {
                denied.add(topic);
            }
        }
        for (OffsetCommitRequestData.OffsetCommitRequestTopic topic : denied) {
            data.topics().remove(topic);
            if (state == null) {
                state = context.state(OffsetCommitAuthState.class);
                if (state == null) {
                    state = new OffsetCommitAuthState();
                    context.state(OffsetCommitAuthState.class, state);
                }
            }
            state.recordDenied(topic.name(), partitionIndices(topic));
        }
        if (state != null && data.topics().isEmpty()) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    topicDenialResponse(state.deniedPartitionsByTopic(), Errors.TOPIC_AUTHORIZATION_FAILED)));
        }
    }

    @Override
    public void onResponse(GatewayContext context, OffsetCommitResponseData data) {
        OffsetCommitAuthState state = context.state(OffsetCommitAuthState.class);
        if (state != null && !state.deniedPartitionsByTopic().isEmpty()) {
            mergeDenials(data, state);
        }
    }

    private static void mergeDenials(OffsetCommitResponseData data, OffsetCommitAuthState state) {
        for (Map.Entry<String, List<Integer>> entry : state.deniedPartitionsByTopic().entrySet()) {
            data.topics().add(topicDenialResponse(entry.getKey(), entry.getValue(), Errors.TOPIC_AUTHORIZATION_FAILED));
        }
    }

    private static OffsetCommitResponseData groupDenialResponse(OffsetCommitRequestData data, Errors error) {
        var response = new OffsetCommitResponseData();
        if (data != null) {
            for (OffsetCommitRequestData.OffsetCommitRequestTopic topic : data.topics()) {
                response.topics().add(topicDenialResponse(topic.name(), partitionIndices(topic), error));
            }
        }
        return response;
    }

    private static OffsetCommitResponseData topicDenialResponse(Map<String, List<Integer>> deniedByTopic, Errors error) {
        var response = new OffsetCommitResponseData();
        for (Map.Entry<String, List<Integer>> entry : deniedByTopic.entrySet()) {
            response.topics().add(topicDenialResponse(entry.getKey(), entry.getValue(), error));
        }
        return response;
    }

    private static OffsetCommitResponseData.OffsetCommitResponseTopic topicDenialResponse(
            String topic,
            List<Integer> partitions,
            Errors error
    ) {
        var topicResponse = new OffsetCommitResponseData.OffsetCommitResponseTopic().setName(topic);
        var partitionResponses = new ArrayList<OffsetCommitResponseData.OffsetCommitResponsePartition>();
        for (int index : partitions) {
            partitionResponses.add(new OffsetCommitResponseData.OffsetCommitResponsePartition()
                    .setPartitionIndex(index)
                    .setErrorCode(error.code()));
        }
        topicResponse.setPartitions(partitionResponses);
        return topicResponse;
    }

    private static List<Integer> partitionIndices(OffsetCommitRequestData.OffsetCommitRequestTopic topic) {
        return topic.partitions().stream()
                .map(OffsetCommitRequestData.OffsetCommitRequestPartition::partitionIndex)
                .toList();
    }
}
