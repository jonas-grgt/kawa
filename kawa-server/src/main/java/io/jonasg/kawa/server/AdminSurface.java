package io.jonasg.kawa.server;

import io.jonasg.kawa.config.AdminConfig;
import io.jonasg.kawa.config.BrokerAuthConfig;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.http.AdminHttpServer;
import io.jonasg.kawa.http.KafkaTopicAdmin;

/// The admin HTTP surface: the [KafkaTopicAdmin] that creates/deletes physical topics on the
/// broker and the [AdminHttpServer] that exposes the gateway's state and dynamic config to a
/// UI. Config writes go through the [DynamicGatewayState]'s config manager to the config topic.
public final class AdminSurface implements AutoCloseable {

    private final AdminHttpServer server;

    public AdminSurface(AdminConfig config, DynamicGatewayState state, MetadataCache cache,
                        String bootstrapServers, BrokerAuthConfig brokerAuth, String passwordSalt) {
        var topicAdmin = new KafkaTopicAdmin(bootstrapServers, brokerAuth);
        server = new AdminHttpServer(config, state.virtualTopics(), cache, state.configManager(),
                state.governance(), topicAdmin, passwordSalt);
    }

    public void start() throws InterruptedException {
        server.start();
    }

    public int boundPort() {
        return server.boundPort();
    }

    @Override
    public void close() {
        server.stop(); // AdminHttpServer.stop() also closes the TopicAdmin
    }
}
