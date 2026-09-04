package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Gates OffsetForLeaderEpoch per topic on TOPIC DESCRIBE. Authorized topics are forwarded;
/// denied topics are stripped from the request so the broker never answers them, and remembered
/// so the response can be reconstructed with a TOPIC_AUTHORIZATION_FAILED partition response per
/// denied partition. If every topic is denied there is nothing to forward, so the request is
/// short-circuited with the fully synthesized denial response. A request without an
/// authenticated principal is denied rather than passed through, so skipping SASL cannot bypass
/// RBAC.
///
/// Note: unlike its siblings (ListOffsets, DeleteRecords), this apiKey's generated classes use
/// `.topic()`/`.partition()` accessors rather than `.name()`/`.partitionIndex()` - do not
/// copy-paste the sibling accessors here.
public final class OffsetForLeaderEpochAuthorizationCheck implements AuthorizationCheck<OffsetForLeaderEpochRequestData, OffsetForLeaderEpochResponseData> {

    private final RbacAuthorizer authorizer;

    public OffsetForLeaderEpochAuthorizationCheck(RbacAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public short apiKey() {
        return ApiKeys.OFFSET_FOR_LEADER_EPOCH.id;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, OffsetForLeaderEpochRequestData data) {
        String principal = context.principal();
        if (principal == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    offsetForLeaderEpochDenialResponse(allPartitionsByTopic(data), Errors.SASL_AUTHENTICATION_FAILED)));
            return;
        }
        if (data == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, new OffsetForLeaderEpochResponseData()));
            return;
        }
        OffsetForLeaderEpochAuthState state = null;
        var denied = new ArrayList<OffsetForLeaderEpochRequestData.OffsetForLeaderTopic>();
        for (OffsetForLeaderEpochRequestData.OffsetForLeaderTopic topic : data.topics()) {
            if (!authorizer.isAuthorized(principal, ResourceType.TOPIC, topic.topic(), AclOperation.DESCRIBE)) {
                denied.add(topic);
            }
        }
        for (OffsetForLeaderEpochRequestData.OffsetForLeaderTopic topic : denied) {
            data.topics().remove(topic);
            if (state == null) {
                state = context.state(OffsetForLeaderEpochAuthState.class);
                if (state == null) {
                    state = new OffsetForLeaderEpochAuthState();
                    context.state(OffsetForLeaderEpochAuthState.class, state);
                }
            }
            state.recordDenied(topic.topic(), partitionIndices(topic));
        }
        if (state != null && data.topics().isEmpty()) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    offsetForLeaderEpochDenialResponse(state.deniedPartitionsByTopic(), Errors.TOPIC_AUTHORIZATION_FAILED)));
        }
    }

    @Override
    public void onResponse(GatewayContext context, OffsetForLeaderEpochResponseData data) {
        OffsetForLeaderEpochAuthState state = context.state(OffsetForLeaderEpochAuthState.class);
        if (state != null && !state.deniedPartitionsByTopic().isEmpty()) {
            mergeDenials(data, state);
        }
    }

    private static void mergeDenials(OffsetForLeaderEpochResponseData data, OffsetForLeaderEpochAuthState state) {
        for (Map.Entry<String, List<Integer>> entry : state.deniedPartitionsByTopic().entrySet()) {
            data.topics().add(topicDenialResponse(entry.getKey(), entry.getValue(), Errors.TOPIC_AUTHORIZATION_FAILED));
        }
    }

    private static OffsetForLeaderEpochResponseData offsetForLeaderEpochDenialResponse(Map<String, List<Integer>> deniedByTopic, Errors error) {
        var response = new OffsetForLeaderEpochResponseData();
        for (Map.Entry<String, List<Integer>> entry : deniedByTopic.entrySet()) {
            response.topics().add(topicDenialResponse(entry.getKey(), entry.getValue(), error));
        }
        return response;
    }

    private static OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult topicDenialResponse(
            String topic,
            List<Integer> partitions,
            Errors error
    ) {
        var topicResponse = new OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult().setTopic(topic);
        var partitionResponses = new ArrayList<OffsetForLeaderEpochResponseData.EpochEndOffset>();
        for (int index : partitions) {
            partitionResponses.add(new OffsetForLeaderEpochResponseData.EpochEndOffset()
                    .setPartition(index)
                    .setErrorCode(error.code()));
        }
        topicResponse.setPartitions(partitionResponses);
        return topicResponse;
    }

    private static List<Integer> partitionIndices(OffsetForLeaderEpochRequestData.OffsetForLeaderTopic topic) {
        return topic.partitions().stream()
                .map(OffsetForLeaderEpochRequestData.OffsetForLeaderPartition::partition)
                .toList();
    }

    private static Map<String, List<Integer>> allPartitionsByTopic(OffsetForLeaderEpochRequestData data) {
        var result = new LinkedHashMap<String, List<Integer>>();
        if (data != null) {
            for (OffsetForLeaderEpochRequestData.OffsetForLeaderTopic topic : data.topics()) {
                result.put(topic.topic(), partitionIndices(topic));
            }
        }
        return result;
    }
}
