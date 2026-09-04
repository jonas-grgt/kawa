package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Gates AddPartitionsToTxn on two resources: a whole-request TRANSACTIONAL_ID WRITE check on the
/// request's transactional id, and a per-topic TOPIC WRITE check on each topic. If the
/// transactional-id gate fails (or there is no authenticated principal) the whole request is
/// short-circuited with a TRANSACTIONAL_ID_AUTHORIZATION_FAILED (or SASL_AUTHENTICATION_FAILED)
/// response for every topic/partition - nothing can proceed without transactional-id access, so
/// the per-topic checks are skipped entirely.
///
/// The response's top-level errorCode field did not exist on the wire for the v0-3 versions kawa
/// decodes (it arrived with the KIP-890 batched transaction format in v4+; verified by an encode
/// round-trip in KafkaBodyCodecTest), so both whole-request and per-topic denials must be
/// expressed per topic/partition.
///
/// Only the v3AndBelow field names are read: kawa registers this API for v0-3, and the batched
/// `transactions()` collection is always empty there (and must not be gated on).
public final class AddPartitionsToTxnAuthorizationCheck implements AuthorizationCheck<AddPartitionsToTxnRequestData, AddPartitionsToTxnResponseData> {

    private final RbacAuthorizer authorizer;

    public AddPartitionsToTxnAuthorizationCheck(RbacAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public short apiKey() {
        return ApiKeys.ADD_PARTITIONS_TO_TXN.id;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, AddPartitionsToTxnRequestData data) {
        String principal = context.principal();
        if (principal == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    txnDenialResponse(data, Errors.SASL_AUTHENTICATION_FAILED)));
            return;
        }
        if (data == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, new AddPartitionsToTxnResponseData()));
            return;
        }
        // Gate 1: TRANSACTIONAL_ID WRITE on the transactional id. All-or-nothing.
        if (!authorizer.isAuthorized(principal, ResourceType.TRANSACTIONAL_ID,
                data.v3AndBelowTransactionalId(), AclOperation.WRITE)) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    txnDenialResponse(data, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)));
            return;
        }
        // Gate 2: per-topic TOPIC WRITE.
        AddPartitionsToTxnAuthState state = null;
        var denied = new ArrayList<AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic>();
        for (AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic topic : data.v3AndBelowTopics()) {
            if (!authorizer.isAuthorized(principal, ResourceType.TOPIC, topic.name(), AclOperation.WRITE)) {
                denied.add(topic);
            }
        }
        for (AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic topic : denied) {
            data.v3AndBelowTopics().remove(topic);
            if (state == null) {
                state = context.state(AddPartitionsToTxnAuthState.class);
                if (state == null) {
                    state = new AddPartitionsToTxnAuthState();
                    context.state(AddPartitionsToTxnAuthState.class, state);
                }
            }
            state.recordDenied(topic.name(), topic.partitions());
        }
        if (state != null && data.v3AndBelowTopics().isEmpty()) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    topicDenialResponse(state.deniedPartitionsByTopic(), Errors.TOPIC_AUTHORIZATION_FAILED)));
        }
    }

    @Override
    public void onResponse(GatewayContext context, AddPartitionsToTxnResponseData data) {
        AddPartitionsToTxnAuthState state = context.state(AddPartitionsToTxnAuthState.class);
        if (state != null && !state.deniedPartitionsByTopic().isEmpty()) {
            mergeDenials(data, state);
        }
    }

    private static void mergeDenials(AddPartitionsToTxnResponseData data, AddPartitionsToTxnAuthState state) {
        for (Map.Entry<String, List<Integer>> entry : state.deniedPartitionsByTopic().entrySet()) {
            data.resultsByTopicV3AndBelow().add(topicDenialResponse(entry.getKey(), entry.getValue(), Errors.TOPIC_AUTHORIZATION_FAILED));
        }
    }

    private static AddPartitionsToTxnResponseData txnDenialResponse(AddPartitionsToTxnRequestData data, Errors error) {
        var response = new AddPartitionsToTxnResponseData();
        if (data != null) {
            for (AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic topic : data.v3AndBelowTopics()) {
                response.resultsByTopicV3AndBelow().add(topicDenialResponse(topic.name(), topic.partitions(), error));
            }
        }
        return response;
    }

    private static AddPartitionsToTxnResponseData topicDenialResponse(Map<String, List<Integer>> deniedByTopic, Errors error) {
        var response = new AddPartitionsToTxnResponseData();
        for (Map.Entry<String, List<Integer>> entry : deniedByTopic.entrySet()) {
            response.resultsByTopicV3AndBelow().add(topicDenialResponse(entry.getKey(), entry.getValue(), error));
        }
        return response;
    }

    private static AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult topicDenialResponse(
            String topic,
            List<Integer> partitions,
            Errors error
    ) {
        var topicResponse = new AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult().setName(topic);
        for (int index : partitions) {
            topicResponse.resultsByPartition().add(new AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResult()
                    .setPartitionIndex(index)
                    .setPartitionErrorCode(error.code()));
        }
        return topicResponse;
    }
}
