package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Gates OffsetDelete on two resources: a whole-request GROUP DELETE check on the request's
/// groupId, and a per-topic TOPIC READ check on each topic. If the group gate fails (or there is
/// no authenticated principal) the whole request is short-circuited with a top-level
/// GROUP_AUTHORIZATION_FAILED (or SASL_AUTHENTICATION_FAILED) response - nothing can proceed
/// without group access, so the per-topic checks are skipped entirely. Otherwise authorized topics
/// are forwarded, denied topics are stripped from the request and remembered so the response can
/// be reconstructed with a TOPIC_AUTHORIZATION_FAILED partition response per denied partition.
public final class OffsetDeleteAuthorizationCheck implements AuthorizationCheck<OffsetDeleteRequestData, OffsetDeleteResponseData> {

    private final RbacAuthorizer authorizer;

    public OffsetDeleteAuthorizationCheck(RbacAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public short apiKey() {
        return ApiKeys.OFFSET_DELETE.id;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, OffsetDeleteRequestData data) {
        String principal = context.principal();
        if (principal == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    new OffsetDeleteResponseData().setErrorCode(Errors.SASL_AUTHENTICATION_FAILED.code())));
            return;
        }
        if (data == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, new OffsetDeleteResponseData()));
            return;
        }
        // Gate 1: GROUP DELETE on the groupId. All-or-nothing - if denied, nothing can proceed.
        if (!authorizer.isAuthorized(principal, ResourceType.GROUP, data.groupId(), AclOperation.DELETE)) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    new OffsetDeleteResponseData().setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code())));
            return;
        }
        // Gate 2: per-topic TOPIC READ.
        OffsetDeleteAuthState state = null;
        var denied = new ArrayList<OffsetDeleteRequestData.OffsetDeleteRequestTopic>();
        for (OffsetDeleteRequestData.OffsetDeleteRequestTopic topic : data.topics()) {
            if (!authorizer.isAuthorized(principal, ResourceType.TOPIC, topic.name(), AclOperation.READ)) {
                denied.add(topic);
            }
        }
        for (OffsetDeleteRequestData.OffsetDeleteRequestTopic topic : denied) {
            data.topics().remove(topic);
            if (state == null) {
                state = context.state(OffsetDeleteAuthState.class);
                if (state == null) {
                    state = new OffsetDeleteAuthState();
                    context.state(OffsetDeleteAuthState.class, state);
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
    public void onResponse(GatewayContext context, OffsetDeleteResponseData data) {
        OffsetDeleteAuthState state = context.state(OffsetDeleteAuthState.class);
        if (state != null && !state.deniedPartitionsByTopic().isEmpty()) {
            mergeDenials(data, state);
        }
    }

    private static void mergeDenials(OffsetDeleteResponseData data, OffsetDeleteAuthState state) {
        for (Map.Entry<String, List<Integer>> entry : state.deniedPartitionsByTopic().entrySet()) {
            data.topics().add(topicDenialResponse(entry.getKey(), entry.getValue(), Errors.TOPIC_AUTHORIZATION_FAILED));
        }
    }

    private static OffsetDeleteResponseData topicDenialResponse(Map<String, List<Integer>> deniedByTopic, Errors error) {
        var response = new OffsetDeleteResponseData();
        for (Map.Entry<String, List<Integer>> entry : deniedByTopic.entrySet()) {
            response.topics().add(topicDenialResponse(entry.getKey(), entry.getValue(), error));
        }
        return response;
    }

    private static OffsetDeleteResponseData.OffsetDeleteResponseTopic topicDenialResponse(
            String topic,
            List<Integer> partitions,
            Errors error
    ) {
        var topicResponse = new OffsetDeleteResponseData.OffsetDeleteResponseTopic().setName(topic);
        for (int index : partitions) {
            topicResponse.partitions().add(new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
                    .setPartitionIndex(index)
                    .setErrorCode(error.code()));
        }
        return topicResponse;
    }

    private static List<Integer> partitionIndices(OffsetDeleteRequestData.OffsetDeleteRequestTopic topic) {
        return topic.partitions().stream()
                .map(OffsetDeleteRequestData.OffsetDeleteRequestPartition::partitionIndex)
                .toList();
    }
}
