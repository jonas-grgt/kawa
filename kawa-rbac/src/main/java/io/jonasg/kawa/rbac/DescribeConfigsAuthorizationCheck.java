package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/// Gates DescribeConfigs on TOPIC DESCRIBE_CONFIGS for TOPIC-typed resources only. Authorized
/// TOPIC resources are forwarded; denied TOPIC resources are stripped from the request so the
/// broker never answers them, and remembered so the response can be reconstructed with a
/// TOPIC_AUTHORIZATION_FAILED result per denied resource. Non-TOPIC resources (BROKER,
/// BROKER_LOGGER) always pass through untouched - cluster-level config authorization is a
/// separate design question and out of scope. If every TOPIC resource is denied there is
/// nothing to forward, so the request is short-circuited with the fully synthesized denial
/// response. A request without an authenticated principal is denied rather than passed through,
/// so skipping SASL cannot bypass RBAC.
public final class DescribeConfigsAuthorizationCheck implements AuthorizationCheck<DescribeConfigsRequestData, DescribeConfigsResponseData> {

    private final RbacAuthorizer authorizer;

    public DescribeConfigsAuthorizationCheck(RbacAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public short apiKey() {
        return ApiKeys.DESCRIBE_CONFIGS.id;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, DescribeConfigsRequestData data) {
        String principal = context.principal();
        if (principal == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    describeConfigsDenialResponse(allTopicResourceNames(data), Errors.SASL_AUTHENTICATION_FAILED)));
            return;
        }
        if (data == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, new DescribeConfigsResponseData()));
            return;
        }
        DescribeConfigsAuthState state = null;
        var denied = new ArrayList<DescribeConfigsRequestData.DescribeConfigsResource>();
        for (DescribeConfigsRequestData.DescribeConfigsResource resource : data.resources()) {
            if (resource.resourceType() != ConfigResource.Type.TOPIC.id()) {
                continue; // non-TOPIC resources are never gated in this slice
            }
            if (!authorizer.isAuthorized(principal, ResourceType.TOPIC, resource.resourceName(), AclOperation.DESCRIBE_CONFIGS)) {
                denied.add(resource);
            }
        }
        for (DescribeConfigsRequestData.DescribeConfigsResource resource : denied) {
            data.resources().remove(resource);
            if (state == null) {
                state = context.state(DescribeConfigsAuthState.class);
                if (state == null) {
                    state = new DescribeConfigsAuthState();
                    context.state(DescribeConfigsAuthState.class, state);
                }
            }
            state.recordDenied(resource.resourceName());
        }
        if (state != null && data.resources().isEmpty()) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    describeConfigsDenialResponse(state.deniedResourceNames(), Errors.TOPIC_AUTHORIZATION_FAILED)));
        }
    }

    @Override
    public void onResponse(GatewayContext context, DescribeConfigsResponseData data) {
        DescribeConfigsAuthState state = context.state(DescribeConfigsAuthState.class);
        if (state != null && !state.deniedResourceNames().isEmpty()) {
            mergeDenials(data, state);
        }
    }

    private static void mergeDenials(DescribeConfigsResponseData data, DescribeConfigsAuthState state) {
        for (String name : state.deniedResourceNames()) {
            data.results().add(resourceDenialResponse(name, Errors.TOPIC_AUTHORIZATION_FAILED));
        }
    }

    private static DescribeConfigsResponseData describeConfigsDenialResponse(Collection<String> deniedResourceNames, Errors error) {
        var response = new DescribeConfigsResponseData();
        for (String name : deniedResourceNames) {
            response.results().add(resourceDenialResponse(name, error));
        }
        return response;
    }

    private static DescribeConfigsResponseData.DescribeConfigsResult resourceDenialResponse(String name, Errors error) {
        return new DescribeConfigsResponseData.DescribeConfigsResult()
                .setResourceType(ConfigResource.Type.TOPIC.id())
                .setResourceName(name)
                .setErrorCode(error.code());
    }

    private static List<String> allTopicResourceNames(DescribeConfigsRequestData data) {
        var result = new ArrayList<String>();
        if (data != null) {
            for (DescribeConfigsRequestData.DescribeConfigsResource resource : data.resources()) {
                if (resource.resourceType() == ConfigResource.Type.TOPIC.id()) {
                    result.add(resource.resourceName());
                }
            }
        }
        return result;
    }
}
