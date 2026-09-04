package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Gates Produce per topic on TOPIC WRITE. Authorized topics are forwarded; denied topics are
/// stripped from the request so their record batches never reach the broker, and remembered so
/// the response can be reconstructed with a TOPIC_AUTHORIZATION_FAILED partition response per
/// denied partition. If every topic is denied there is nothing to forward, so the request is
/// short-circuited with the fully synthesized denial response. A request without an
/// authenticated principal is denied rather than passed through, so skipping SASL cannot
/// bypass RBAC.
public final class ProduceAuthorizationCheck implements AuthorizationCheck<ProduceRequestData, ProduceResponseData> {

    private final RbacAuthorizer authorizer;

    public ProduceAuthorizationCheck(RbacAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public short apiKey() {
        return (short) ApiKeys.PRODUCE.id;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, ProduceRequestData data) {
        String principal = context.principal();
        if (principal == null) {
            // Unauthenticated: deny the whole request rather than bypass RBAC.
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    produceDenialResponse(allPartitionsByTopic(data), Errors.SASL_AUTHENTICATION_FAILED)));
            return;
        }
        if (data == null) {
            // Undecoded body (e.g. wire version outside the registered range): nothing to
            // enumerate, so deny with an empty response rather than crash.
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    new ProduceResponseData()));
            return;
        }
        ProduceAuthState state = null;
        var denied = new ArrayList<ProduceRequestData.TopicProduceData>();
        for (ProduceRequestData.TopicProduceData topic : data.topicData()) {
            if (!authorizer.isAuthorized(principal, ResourceType.TOPIC, topic.name(), AclOperation.WRITE)) {
                denied.add(topic);
            }
        }
        for (ProduceRequestData.TopicProduceData topic : denied) {
            data.topicData().remove(topic);
            if (state == null) {
                state = context.state(ProduceAuthState.class);
                if (state == null) {
                    state = new ProduceAuthState();
                    context.state(ProduceAuthState.class, state);
                }
            }
            state.recordDenied(topic.name(), partitionIndices(topic));
        }
        if (state != null && data.topicData().isEmpty()) {
            // Every topic denied: nothing to forward, answer locally with the denial response.
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    produceDenialResponse(state.deniedPartitionsByTopic(), Errors.TOPIC_AUTHORIZATION_FAILED)));
        }
    }

    @Override
    public void onResponse(GatewayContext context, ProduceResponseData data) {
        ProduceAuthState state = context.state(ProduceAuthState.class);
        if (state != null && !state.deniedPartitionsByTopic().isEmpty()) {
            mergeDenials(data, state);
        }
    }

    private static void mergeDenials(ProduceResponseData data, ProduceAuthState state) {
        for (Map.Entry<String, List<Integer>> entry : state.deniedPartitionsByTopic().entrySet()) {
            data.responses().add(topicDenialResponse(entry.getKey(), entry.getValue(), Errors.TOPIC_AUTHORIZATION_FAILED));
        }
    }

    private static ProduceResponseData produceDenialResponse(Map<String, List<Integer>> deniedByTopic, Errors error) {
        var response = new ProduceResponseData();
        for (Map.Entry<String, List<Integer>> entry : deniedByTopic.entrySet()) {
            response.responses().add(topicDenialResponse(entry.getKey(), entry.getValue(), error));
        }
        return response;
    }

    private static ProduceResponseData.TopicProduceResponse topicDenialResponse(
            String topic,
            List<Integer> partitions,
            Errors error
    ) {
        var topicResponse = new ProduceResponseData.TopicProduceResponse().setName(topic);
        var partitionResponses = new ArrayList<ProduceResponseData.PartitionProduceResponse>();
        for (int index : partitions) {
            partitionResponses.add(new ProduceResponseData.PartitionProduceResponse()
                    .setIndex(index)
                    .setErrorCode(error.code())
                    .setBaseOffset(-1L));
        }
        topicResponse.setPartitionResponses(partitionResponses);
        return topicResponse;
    }

    private static List<Integer> partitionIndices(ProduceRequestData.TopicProduceData topic) {
        return topic.partitionData().stream()
                .map(ProduceRequestData.PartitionProduceData::index)
                .toList();
    }

    private static Map<String, List<Integer>> allPartitionsByTopic(Object body) {
        var result = new LinkedHashMap<String, List<Integer>>();
        if (body instanceof ProduceRequestData data) {
            for (ProduceRequestData.TopicProduceData topic : data.topicData()) {
                result.put(topic.name(), partitionIndices(topic));
            }
        }
        return result;
    }
}
