package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.List;

/// Gates CreatePartitions per topic on TOPIC ALTER. Authorized topics are forwarded; denied topics
/// are stripped from the request so the broker never enlarges them, and remembered so the response
/// can be reconstructed with a TOPIC_AUTHORIZATION_FAILED entry per denied topic. If every topic is
/// denied there is nothing to forward, so the request is short-circuited with the fully synthesized
/// denial response. A request without an authenticated principal is denied rather than passed
/// through, so skipping SASL cannot bypass RBAC.
public final class CreatePartitionsAuthorizationCheck implements AuthorizationCheck<CreatePartitionsRequestData, CreatePartitionsResponseData> {

    private final RbacAuthorizer authorizer;

    public CreatePartitionsAuthorizationCheck(RbacAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public short apiKey() {
        return ApiKeys.CREATE_PARTITIONS.id;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, CreatePartitionsRequestData data) {
        String principal = context.principal();
        if (principal == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    createPartitionsDenialResponse(allTopicNames(data), Errors.SASL_AUTHENTICATION_FAILED)));
            return;
        }
        if (data == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, new CreatePartitionsResponseData()));
            return;
        }
        CreatePartitionsAuthState state = null;
        var denied = new ArrayList<CreatePartitionsRequestData.CreatePartitionsTopic>();
        for (CreatePartitionsRequestData.CreatePartitionsTopic topic : data.topics()) {
            if (!authorizer.isAuthorized(principal, ResourceType.TOPIC, topic.name(), AclOperation.ALTER)) {
                denied.add(topic);
            }
        }
        for (CreatePartitionsRequestData.CreatePartitionsTopic topic : denied) {
            data.topics().remove(topic);
        }
        if (!denied.isEmpty()) {
            state = context.state(CreatePartitionsAuthState.class);
            if (state == null) {
                state = new CreatePartitionsAuthState();
                context.state(CreatePartitionsAuthState.class, state);
            }
            state.recordDenied(denied.stream().map(CreatePartitionsRequestData.CreatePartitionsTopic::name).toList());
        }
        if (state != null && data.topics().isEmpty()) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    createPartitionsDenialResponse(state.deniedTopics(), Errors.TOPIC_AUTHORIZATION_FAILED)));
        }
    }

    @Override
    public void onResponse(GatewayContext context, CreatePartitionsResponseData data) {
        CreatePartitionsAuthState state = context.state(CreatePartitionsAuthState.class);
        if (state != null && !state.deniedTopics().isEmpty()) {
            for (String topic : state.deniedTopics()) {
                data.results().add(topicDenialResponse(topic, Errors.TOPIC_AUTHORIZATION_FAILED));
            }
        }
    }

    private static CreatePartitionsResponseData createPartitionsDenialResponse(List<String> topics, Errors error) {
        var response = new CreatePartitionsResponseData();
        for (String topic : topics) {
            response.results().add(topicDenialResponse(topic, error));
        }
        return response;
    }

    private static CreatePartitionsResponseData.CreatePartitionsTopicResult topicDenialResponse(String topic, Errors error) {
        return new CreatePartitionsResponseData.CreatePartitionsTopicResult()
                .setName(topic)
                .setErrorCode(error.code())
                .setErrorMessage(error.message());
    }

    private static List<String> allTopicNames(CreatePartitionsRequestData data) {
        var result = new ArrayList<String>();
        if (data != null) {
            for (CreatePartitionsRequestData.CreatePartitionsTopic topic : data.topics()) {
                result.add(topic.name());
            }
        }
        return result;
    }
}
