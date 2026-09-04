package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Gates ListOffsets per topic on TOPIC DESCRIBE. Authorized topics are forwarded; denied
/// topics are stripped from the request so the broker never answers them, and remembered so the
/// response can be reconstructed with a TOPIC_AUTHORIZATION_FAILED partition response per denied
/// partition. If every topic is denied there is nothing to forward, so the request is
/// short-circuited with the fully synthesized denial response. A request without an
/// authenticated principal is denied rather than passed through, so skipping SASL cannot bypass
/// RBAC.
public final class ListOffsetsAuthorizationCheck implements AuthorizationCheck<ListOffsetsRequestData, ListOffsetsResponseData> {

    private final RbacAuthorizer authorizer;

    public ListOffsetsAuthorizationCheck(RbacAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public short apiKey() {
        return ApiKeys.LIST_OFFSETS.id;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, ListOffsetsRequestData data) {
        String principal = context.principal();
        if (principal == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    listOffsetsDenialResponse(allPartitionsByTopic(data), Errors.SASL_AUTHENTICATION_FAILED)));
            return;
        }
        if (data == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, new ListOffsetsResponseData()));
            return;
        }
        ListOffsetsAuthState state = null;
        var denied = new ArrayList<ListOffsetsRequestData.ListOffsetsTopic>();
        for (ListOffsetsRequestData.ListOffsetsTopic topic : data.topics()) {
            if (!authorizer.isAuthorized(principal, ResourceType.TOPIC, topic.name(), AclOperation.DESCRIBE)) {
                denied.add(topic);
            }
        }
        for (ListOffsetsRequestData.ListOffsetsTopic topic : denied) {
            data.topics().remove(topic);
            if (state == null) {
                state = context.state(ListOffsetsAuthState.class);
                if (state == null) {
                    state = new ListOffsetsAuthState();
                    context.state(ListOffsetsAuthState.class, state);
                }
            }
            state.recordDenied(topic.name(), partitionIndices(topic));
        }
        if (state != null && data.topics().isEmpty()) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    listOffsetsDenialResponse(state.deniedPartitionsByTopic(), Errors.TOPIC_AUTHORIZATION_FAILED)));
        }
    }

    @Override
    public void onResponse(GatewayContext context, ListOffsetsResponseData data) {
        ListOffsetsAuthState state = context.state(ListOffsetsAuthState.class);
        if (state != null && !state.deniedPartitionsByTopic().isEmpty()) {
            mergeDenials(data, state);
        }
    }

    private static void mergeDenials(ListOffsetsResponseData data, ListOffsetsAuthState state) {
        for (Map.Entry<String, List<Integer>> entry : state.deniedPartitionsByTopic().entrySet()) {
            data.topics().add(topicDenialResponse(entry.getKey(), entry.getValue(), Errors.TOPIC_AUTHORIZATION_FAILED));
        }
    }

    private static ListOffsetsResponseData listOffsetsDenialResponse(Map<String, List<Integer>> deniedByTopic, Errors error) {
        var response = new ListOffsetsResponseData();
        for (Map.Entry<String, List<Integer>> entry : deniedByTopic.entrySet()) {
            response.topics().add(topicDenialResponse(entry.getKey(), entry.getValue(), error));
        }
        return response;
    }

    private static ListOffsetsResponseData.ListOffsetsTopicResponse topicDenialResponse(
            String topic,
            List<Integer> partitions,
            Errors error
    ) {
        var topicResponse = new ListOffsetsResponseData.ListOffsetsTopicResponse().setName(topic);
        var partitionResponses = new ArrayList<ListOffsetsResponseData.ListOffsetsPartitionResponse>();
        for (int index : partitions) {
            partitionResponses.add(new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                    .setPartitionIndex(index)
                    .setErrorCode(error.code()));
        }
        topicResponse.setPartitions(partitionResponses);
        return topicResponse;
    }

    private static List<Integer> partitionIndices(ListOffsetsRequestData.ListOffsetsTopic topic) {
        return topic.partitions().stream()
                .map(ListOffsetsRequestData.ListOffsetsPartition::partitionIndex)
                .toList();
    }

    private static Map<String, List<Integer>> allPartitionsByTopic(ListOffsetsRequestData data) {
        var result = new LinkedHashMap<String, List<Integer>>();
        if (data != null) {
            for (ListOffsetsRequestData.ListOffsetsTopic topic : data.topics()) {
                result.put(topic.name(), partitionIndices(topic));
            }
        }
        return result;
    }
}
