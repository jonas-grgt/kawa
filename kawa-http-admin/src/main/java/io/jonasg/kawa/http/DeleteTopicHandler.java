package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.core.cluster.MetadataCache;

/// Serves `DELETE /topics/{name}`: removes a virtual topic's config when `name` is a virtual
/// topic, otherwise deletes the physical topic on the broker through the [TopicAdmin].
/// A name that is both virtual and physical resolves to the virtual config removal (the
/// safer operation - no broker data loss).
public final class DeleteTopicHandler implements Router.Handler {

    private final GatewayConfigRepository repository;
    private final MetadataCache cache;
    private final TopicAdmin topicAdmin;

    public DeleteTopicHandler(GatewayConfigRepository repository, MetadataCache cache, TopicAdmin topicAdmin) {
        this.repository = repository;
        this.cache = cache;
        this.topicAdmin = topicAdmin;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        if (!"DELETE".equals(request.method())) {
            return Router.Response.badRequest("unsupported method " + request.method());
        }
        String name = request.pathParams().get("name");
        GatewayConfig base = repository.getActiveConfigOrEmpty();
        if (base.virtualTopics().containsKey(name)) {
            repository.update(config -> config.removeVirtualTopic(name));
            return Router.Response.noContent();
        }
        boolean physicalExists = cache.topics().stream().anyMatch(t -> t.name().equals(name));
        if (!physicalExists) {
            return Router.Response.notFound("topic '" + name + "' not found");
        }
        try {
            topicAdmin.deleteTopic(name);
            return Router.Response.noContent();
        } catch (Exception e) {
            return Router.Response.internalError("failed to delete topic '" + name + "': " + e.getMessage());
        }
    }
}