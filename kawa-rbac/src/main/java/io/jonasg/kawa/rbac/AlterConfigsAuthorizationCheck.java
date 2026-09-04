package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.core.ShortCircuitResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/// Gates AlterConfigs and IncrementalAlterConfigs on TOPIC ALTER_CONFIGS for TOPIC-typed
/// resources only. Authorized TOPIC resources are forwarded; denied TOPIC resources are stripped
/// from the request so the broker never applies them, and remembered so the response can be
/// reconstructed with a TOPIC_AUTHORIZATION_FAILED result per denied resource. Non-TOPIC
/// resources (BROKER, BROKER_LOGGER) always pass through untouched - cluster-level config
/// authorization is a separate design question and out of scope. If every TOPIC resource is
/// denied there is nothing to forward, so the request is short-circuited with the fully
/// synthesized denial response. A request without an authenticated principal is denied rather
/// than passed through, so skipping SASL cannot bypass RBAC.
///
/// AlterConfigs and IncrementalAlterConfigs share identical generated accessor shapes
/// (`resources()`/`responses()`, `resourceType()`/`resourceName()`/`setErrorCode()`), so a
/// single generic implementation covers both; the concrete request/response/resource types are
/// supplied at the construction site.
public final class AlterConfigsAuthorizationCheck<Req, Resp, ReqResource, RespResource>
        implements AuthorizationCheck<Req, Resp> {

    private final short apiKey;
    private final RbacAuthorizer authorizer;
    private final Supplier<Resp> emptyResponse;
    private final Function<Req, Collection<ReqResource>> requestResources;
    private final Function<ReqResource, Byte> resourceType;
    private final Function<ReqResource, String> resourceName;
    private final Function<Resp, List<RespResource>> responseResources;
    private final BiFunction<String, Short, RespResource> denialEntry;

    public AlterConfigsAuthorizationCheck(
            short apiKey,
            RbacAuthorizer authorizer,
            Supplier<Resp> emptyResponse,
            Function<Req, Collection<ReqResource>> requestResources,
            Function<ReqResource, Byte> resourceType,
            Function<ReqResource, String> resourceName,
            Function<Resp, List<RespResource>> responseResources,
            BiFunction<String, Short, RespResource> denialEntry
    ) {
        this.apiKey = apiKey;
        this.authorizer = authorizer;
        this.emptyResponse = emptyResponse;
        this.requestResources = requestResources;
        this.resourceType = resourceType;
        this.resourceName = resourceName;
        this.responseResources = responseResources;
        this.denialEntry = denialEntry;
    }

    @Override
    public short apiKey() {
        return apiKey;
    }

    @Override
    public void onRequest(GatewayContext context, short apiVersion, Req data) {
        String principal = context.principal();
        if (principal == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    denialResponse(allTopicResourceNames(data), Errors.SASL_AUTHENTICATION_FAILED)));
            return;
        }
        if (data == null) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion, emptyResponse.get()));
            return;
        }
        AlterConfigsAuthState state = null;
        var denied = new ArrayList<ReqResource>();
        for (ReqResource resource : requestResources.apply(data)) {
            if (resourceType.apply(resource) != ConfigResource.Type.TOPIC.id()) {
                continue; // non-TOPIC resources are never gated in this slice
            }
            if (!authorizer.isAuthorized(principal, ResourceType.TOPIC, resourceName.apply(resource), AclOperation.ALTER_CONFIGS)) {
                denied.add(resource);
            }
        }
        for (ReqResource resource : denied) {
            requestResources.apply(data).remove(resource);
            if (state == null) {
                state = context.state(AlterConfigsAuthState.class);
                if (state == null) {
                    state = new AlterConfigsAuthState();
                    context.state(AlterConfigsAuthState.class, state);
                }
            }
            state.recordDenied(resourceName.apply(resource));
        }
        if (state != null && requestResources.apply(data).isEmpty()) {
            context.shortCircuit(new ShortCircuitResult(apiKey(), apiVersion,
                    denialResponse(state.deniedResourceNames(), Errors.TOPIC_AUTHORIZATION_FAILED)));
        }
    }

    @Override
    public void onResponse(GatewayContext context, Resp data) {
        AlterConfigsAuthState state = context.state(AlterConfigsAuthState.class);
        if (state != null && !state.deniedResourceNames().isEmpty()) {
            for (String name : state.deniedResourceNames()) {
                responseResources.apply(data).add(denialEntry.apply(name, Errors.TOPIC_AUTHORIZATION_FAILED.code()));
            }
        }
    }

    private Resp denialResponse(Collection<String> deniedResourceNames, Errors error) {
        Resp response = emptyResponse.get();
        for (String name : deniedResourceNames) {
            responseResources.apply(response).add(denialEntry.apply(name, error.code()));
        }
        return response;
    }

    private List<String> allTopicResourceNames(Req data) {
        var result = new ArrayList<String>();
        if (data != null) {
            for (ReqResource resource : requestResources.apply(data)) {
                if (resourceType.apply(resource) == ConfigResource.Type.TOPIC.id()) {
                    result.add(resourceName.apply(resource));
                }
            }
        }
        return result;
    }
}
