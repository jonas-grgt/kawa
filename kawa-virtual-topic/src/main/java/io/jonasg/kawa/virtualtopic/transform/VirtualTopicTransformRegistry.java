package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.GatewayContext;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/// Registry for virtual-topic API transforms. Owns the single unchecked cast per
/// direction (`Object → Req` / `Object → Resp`) so that transform
/// implementations stay cast-free.
public final class VirtualTopicTransformRegistry {

    private final Map<Short, VirtualTopicTransform<?, ?>> transforms;

    public VirtualTopicTransformRegistry(Collection<? extends VirtualTopicTransform<?, ?>> transforms) {
        Map<Short, VirtualTopicTransform<?, ?>> byKey = new HashMap<>();
        for (VirtualTopicTransform<?, ?> transform : transforms) {
            if (byKey.put(transform.apiKey(), transform) != null) {
                throw new IllegalArgumentException("Duplicate virtual-topic transform for api key " + transform.apiKey());
            }
        }
        this.transforms = Map.copyOf(byKey);
    }

    @SuppressWarnings("unchecked")
    public void onRequest(GatewayContext context, short apiKey, Object body) {
        if (body == null) {
            return;
        }
        VirtualTopicTransform<Object, Object> transform =
                (VirtualTopicTransform<Object, Object>) transforms.get(apiKey);
        if (transform != null) {
            transform.onRequest(context, body);
        }
    }

    @SuppressWarnings("unchecked")
    public void onResponse(GatewayContext context, short apiKey, Object body) {
        if (body == null) {
            return;
        }
        VirtualTopicTransform<Object, Object> transform =
                (VirtualTopicTransform<Object, Object>) transforms.get(apiKey);
        if (transform != null) {
            transform.onResponse(context, body);
        }
    }

    public boolean hasApiKey(short apiKey) {
        return transforms.containsKey(apiKey);
    }
}
