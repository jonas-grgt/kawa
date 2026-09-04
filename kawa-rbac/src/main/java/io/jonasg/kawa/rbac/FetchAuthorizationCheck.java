package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Gates Fetch per topic on TOPIC READ. Authorized topics are forwarded; denied topics are
/// stripped from the request before it reaches VirtualTopicInterceptor, so they're never
/// forwarded to the broker and never become part of an incremental fetch session (kawa's own
/// FetchSessionRegistry or the broker's), and remembered so the response can be reconstructed
/// with a TOPIC_AUTHORIZATION_FAILED partition response per denied partition. If every topic is
/// denied there is nothing to forward, so the request is short-circuited with the fully
/// synthesized denial response. A request without an authenticated principal is denied rather
/// than passed through, so skipping SASL cannot bypass RBAC.
public final class FetchAuthorizationCheck implements AuthorizationCheck<FetchRequestData, FetchResponseData> {

    private final RbacAuthorizer authorizer;

    public FetchAuthorizationCheck(RbacAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public short apiKey() {
        return ApiKeys.FETCH.id;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, FetchRequestData data) {
        String principal = context.principal();
        if (principal == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    fetchDenialResponse(allPartitionsByTopic(data), Errors.SASL_AUTHENTICATION_FAILED)));
            return;
        }
        if (data == null) {
            // Undecoded body (e.g. wire version outside the registered range): nothing to
            // enumerate, deny with an empty response rather than crash. (This mirrors a bug
            // that had to be fixed in ProduceAuthorizationCheck - don't skip this guard.)
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, new FetchResponseData()));
            return;
        }
        FetchAuthState state = null;
        var denied = new ArrayList<FetchRequestData.FetchTopic>();
        for (FetchRequestData.FetchTopic topic : data.topics()) {
            if (!authorizer.isAuthorized(principal, ResourceType.TOPIC, topic.topic(), AclOperation.READ)) {
                denied.add(topic);
            }
        }
        for (FetchRequestData.FetchTopic topic : denied) {
            data.topics().remove(topic);
            if (state == null) {
                state = context.state(FetchAuthState.class);
                if (state == null) {
                    state = new FetchAuthState();
                    context.state(FetchAuthState.class, state);
                }
            }
            state.recordDenied(topic.topic(), partitionIndices(topic));
        }
        // IMPORTANT: unlike Produce, an empty data.topics() here is the NORMAL steady-state for
        // an incremental fetch (the client omits topics already part of the session), not just
        // an edge case. Only short-circuit when something was actually denied in THIS request -
        // state stays null otherwise, exactly the guard ProduceAuthorizationCheck needed fixed.
        if (state != null && data.topics().isEmpty()) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    fetchDenialResponse(state.deniedPartitionsByTopic(), Errors.TOPIC_AUTHORIZATION_FAILED)));
        }
    }

    @Override
    public void onResponse(GatewayContext context, FetchResponseData data) {
        FetchAuthState state = context.state(FetchAuthState.class);
        if (state != null && !state.deniedPartitionsByTopic().isEmpty()) {
            mergeDenials(data, state);
        }
    }

    private static void mergeDenials(FetchResponseData data, FetchAuthState state) {
        for (Map.Entry<String, List<Integer>> entry : state.deniedPartitionsByTopic().entrySet()) {
            data.responses().add(topicDenialResponse(entry.getKey(), entry.getValue(), Errors.TOPIC_AUTHORIZATION_FAILED));
        }
    }

    private static FetchResponseData fetchDenialResponse(Map<String, List<Integer>> deniedByTopic, Errors error) {
        var response = new FetchResponseData();
        for (Map.Entry<String, List<Integer>> entry : deniedByTopic.entrySet()) {
            response.responses().add(topicDenialResponse(entry.getKey(), entry.getValue(), error));
        }
        return response;
    }

    private static FetchResponseData.FetchableTopicResponse topicDenialResponse(
            String topic,
            List<Integer> partitions,
            Errors error
    ) {
        var topicResponse = new FetchResponseData.FetchableTopicResponse().setTopic(topic);
        var partitionData = new ArrayList<FetchResponseData.PartitionData>();
        for (int index : partitions) {
            partitionData.add(new FetchResponseData.PartitionData()
                    .setPartitionIndex(index)
                    .setErrorCode(error.code()));
        }
        topicResponse.setPartitions(partitionData);
        return topicResponse;
    }

    private static List<Integer> partitionIndices(FetchRequestData.FetchTopic topic) {
        return topic.partitions().stream()
                .map(FetchRequestData.FetchPartition::partition)
                .toList();
    }

    private static Map<String, List<Integer>> allPartitionsByTopic(FetchRequestData data) {
        var result = new LinkedHashMap<String, List<Integer>>();
        if (data != null) {
            for (FetchRequestData.FetchTopic topic : data.topics()) {
                result.put(topic.topic(), partitionIndices(topic));
            }
        }
        return result;
    }
}
