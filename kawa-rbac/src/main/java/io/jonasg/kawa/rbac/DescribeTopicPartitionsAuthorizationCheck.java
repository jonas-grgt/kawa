package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.List;

/// Gates DescribeTopicPartitions on TOPIC DESCRIBE. Unlike Metadata there is no list-all mode -
/// the request always names specific topics (`data.topics()` is never null), and kawa does not
/// decode this API's response (it is opaque `Object` in the virtual-topic transform), so no
/// response-side synthesis or filtering is possible. Denial is therefore handled entirely on the
/// request side: denied topic names are stripped before forwarding, and if that empties the list
/// the whole request is short-circuited with a synthesized UNKNOWN_TOPIC_OR_PARTITION response
/// (mirroring Metadata's visibility semantics - an unauthorized topic behaves as if it doesn't
/// exist). A request without an authenticated principal is denied rather than passed through, so
/// skipping SASL cannot bypass RBAC.
///
/// Known limitation: for a partially-denied request (some topics authorized, some not) the
/// trimmed request is forwarded as-is and the denied topics simply do not appear in the broker's
/// response - kawa cannot inject an explicit UNKNOWN_TOPIC_OR_PARTITION marker into an opaque
/// response body.
public final class DescribeTopicPartitionsAuthorizationCheck implements AuthorizationCheck<DescribeTopicPartitionsRequestData, Object> {

    private final RbacAuthorizer authorizer;

    public DescribeTopicPartitionsAuthorizationCheck(RbacAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public short apiKey() {
        return ApiKeys.DESCRIBE_TOPIC_PARTITIONS.id;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, DescribeTopicPartitionsRequestData data) {
        String principal = context.principal();
        if (principal == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    describeTopicPartitionsDenialResponse(allTopicNames(data), Errors.SASL_AUTHENTICATION_FAILED)));
            return;
        }
        if (data == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, new DescribeTopicPartitionsResponseData()));
            return;
        }
        var denied = new ArrayList<DescribeTopicPartitionsRequestData.TopicRequest>();
        for (DescribeTopicPartitionsRequestData.TopicRequest topic : data.topics()) {
            if (!authorizer.isAuthorized(principal, ResourceType.TOPIC, topic.name(), AclOperation.DESCRIBE)) {
                denied.add(topic);
            }
        }
        for (DescribeTopicPartitionsRequestData.TopicRequest topic : denied) {
            data.topics().remove(topic);
        }
        if (!denied.isEmpty() && data.topics().isEmpty()) {
            // Every topic denied: nothing to forward, answer locally with the denial response.
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    describeTopicPartitionsDenialResponse(deniedNames(denied), Errors.UNKNOWN_TOPIC_OR_PARTITION)));
        }
    }

    private static DescribeTopicPartitionsResponseData describeTopicPartitionsDenialResponse(List<String> deniedTopics, Errors error) {
        var response = new DescribeTopicPartitionsResponseData();
        for (String name : deniedTopics) {
            response.topics().add(new DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic()
                    .setName(name)
                    .setErrorCode(error.code()));
        }
        return response;
    }

    private static List<String> deniedNames(List<DescribeTopicPartitionsRequestData.TopicRequest> denied) {
        return denied.stream().map(DescribeTopicPartitionsRequestData.TopicRequest::name).toList();
    }

    private static List<String> allTopicNames(DescribeTopicPartitionsRequestData data) {
        var result = new ArrayList<String>();
        if (data != null) {
            for (DescribeTopicPartitionsRequestData.TopicRequest topic : data.topics()) {
                result.add(topic.name());
            }
        }
        return result;
    }
}
