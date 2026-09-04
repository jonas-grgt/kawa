package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Gates DeleteRecords per topic on TOPIC DELETE. Authorized topics are forwarded; denied topics
/// are stripped from the request so the broker never deletes their records, and remembered so
/// the response can be reconstructed with a TOPIC_AUTHORIZATION_FAILED partition response per
/// denied partition. If every topic is denied there is nothing to forward, so the request is
/// short-circuited with the fully synthesized denial response. A request without an
/// authenticated principal is denied rather than passed through, so skipping SASL cannot bypass
/// RBAC.
public final class DeleteRecordsAuthorizationCheck implements AuthorizationCheck<DeleteRecordsRequestData, DeleteRecordsResponseData> {

    private final RbacAuthorizer authorizer;

    public DeleteRecordsAuthorizationCheck(RbacAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public short apiKey() {
        return ApiKeys.DELETE_RECORDS.id;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, DeleteRecordsRequestData data) {
        String principal = context.principal();
        if (principal == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    deleteRecordsDenialResponse(allPartitionsByTopic(data), Errors.SASL_AUTHENTICATION_FAILED)));
            return;
        }
        if (data == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, new DeleteRecordsResponseData()));
            return;
        }
        DeleteRecordsAuthState state = null;
        var denied = new ArrayList<DeleteRecordsRequestData.DeleteRecordsTopic>();
        for (DeleteRecordsRequestData.DeleteRecordsTopic topic : data.topics()) {
            if (!authorizer.isAuthorized(principal, ResourceType.TOPIC, topic.name(), AclOperation.DELETE)) {
                denied.add(topic);
            }
        }
        for (DeleteRecordsRequestData.DeleteRecordsTopic topic : denied) {
            data.topics().remove(topic);
            if (state == null) {
                state = context.state(DeleteRecordsAuthState.class);
                if (state == null) {
                    state = new DeleteRecordsAuthState();
                    context.state(DeleteRecordsAuthState.class, state);
                }
            }
            state.recordDenied(topic.name(), partitionIndices(topic));
        }
        if (state != null && data.topics().isEmpty()) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    deleteRecordsDenialResponse(state.deniedPartitionsByTopic(), Errors.TOPIC_AUTHORIZATION_FAILED)));
        }
    }

    @Override
    public void onResponse(GatewayContext context, DeleteRecordsResponseData data) {
        DeleteRecordsAuthState state = context.state(DeleteRecordsAuthState.class);
        if (state != null && !state.deniedPartitionsByTopic().isEmpty()) {
            mergeDenials(data, state);
        }
    }

    private static void mergeDenials(DeleteRecordsResponseData data, DeleteRecordsAuthState state) {
        for (Map.Entry<String, List<Integer>> entry : state.deniedPartitionsByTopic().entrySet()) {
            data.topics().add(topicDenialResponse(entry.getKey(), entry.getValue(), Errors.TOPIC_AUTHORIZATION_FAILED));
        }
    }

    private static DeleteRecordsResponseData deleteRecordsDenialResponse(Map<String, List<Integer>> deniedByTopic, Errors error) {
        var response = new DeleteRecordsResponseData();
        for (Map.Entry<String, List<Integer>> entry : deniedByTopic.entrySet()) {
            response.topics().add(topicDenialResponse(entry.getKey(), entry.getValue(), error));
        }
        return response;
    }

    private static DeleteRecordsResponseData.DeleteRecordsTopicResult topicDenialResponse(
            String topic,
            List<Integer> partitions,
            Errors error
    ) {
        var topicResponse = new DeleteRecordsResponseData.DeleteRecordsTopicResult().setName(topic);
        for (int index : partitions) {
            topicResponse.partitions().add(new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                    .setPartitionIndex(index)
                    .setErrorCode(error.code()));
        }
        return topicResponse;
    }

    private static List<Integer> partitionIndices(DeleteRecordsRequestData.DeleteRecordsTopic topic) {
        return topic.partitions().stream()
                .map(DeleteRecordsRequestData.DeleteRecordsPartition::partitionIndex)
                .toList();
    }

    private static Map<String, List<Integer>> allPartitionsByTopic(DeleteRecordsRequestData data) {
        var result = new LinkedHashMap<String, List<Integer>>();
        if (data != null) {
            for (DeleteRecordsRequestData.DeleteRecordsTopic topic : data.topics()) {
                result.put(topic.name(), partitionIndices(topic));
            }
        }
        return result;
    }
}
