package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.VirtualTopicConfig;

import java.util.Map;

/// Serves `/config/virtual-topics`: lists the virtual topic map, upserts one entry via
/// `PUT /config/virtual-topics/{name}` and removes it via `DELETE /config/virtual-topics/{name}`.
/// Each write persists a full [GatewayConfig] snapshot through the [GatewayConfigRepository].
public final class VirtualTopicsConfigHandler extends ConfigSectionHandler<VirtualTopicConfig> {

    public VirtualTopicsConfigHandler(GatewayConfigRepository repository) {
        super(repository, VirtualTopicConfig.class, "virtual topic");
    }

    @Override
    protected Map<String, VirtualTopicConfig> entries(GatewayConfig config) {
        return config.virtualTopics();
    }

    @Override
    protected GatewayConfig upsert(GatewayConfig config, String name, VirtualTopicConfig value) {
        return config.putVirtualTopic(name, value);
    }

    @Override
    protected GatewayConfig remove(GatewayConfig config, String name) {
        return config.removeVirtualTopic(name);
    }
}