package io.jonasg.kawa.server;

import io.jonasg.kawa.config.ConfigTopicConsumer;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.core.VirtualTopicManager;
import io.jonasg.kawa.rbac.RbacAuthorizer;
import io.jonasg.kawa.server.auth.SaslAuthenticator;

import java.util.Properties;

/// Owns the config-topic consumer and applies each [GatewayConfig] snapshot to the mutable
/// consumers: [VirtualTopicManager], [RbacAuthorizer] and [SaslAuthenticator].
///
/// A snapshot is applied in an order that keeps the application atomic in the failure case:
/// [RbacAuthorizer#reload] is the only consumer that can reject a snapshot (an unknown role
/// reference throws), so it runs first - if it throws, nothing else has been touched and the
/// previous snapshot stays fully in place. The other two reloads cannot throw for a config
/// that already passed [GatewayConfig]'s own validation.
///
/// [awaitInitialLoad] blocks until the config topic has been caught up from the earliest
/// offset, so the gateway can refuse to serve until it has applied the config that existed
/// at boot.
public final class DynamicConfigManager implements AutoCloseable {

    private final ConfigTopicConsumer consumer;
    private final VirtualTopicManager virtualTopics;
    private final RbacAuthorizer authorizer;
    private final SaslAuthenticator saslAuthenticator;

    private volatile GatewayConfig lastConfig;

    public DynamicConfigManager(
            String bootstrapServers,
            String topic,
            String groupId,
            VirtualTopicManager virtualTopics,
            RbacAuthorizer authorizer,
            SaslAuthenticator saslAuthenticator
    ) {
        this(bootstrapServers, topic, groupId, new Properties(), virtualTopics, authorizer, saslAuthenticator);
    }

    /// Variant that accepts extra consumer properties (e.g. SASL/security settings for the
    /// config topic) on top of the base bootstrap/group/deserializer configuration.
    public DynamicConfigManager(
            String bootstrapServers,
            String topic,
            String groupId,
            Properties extraProps,
            VirtualTopicManager virtualTopics,
            RbacAuthorizer authorizer,
            SaslAuthenticator saslAuthenticator
    ) {
        this.virtualTopics = virtualTopics;
        this.authorizer = authorizer;
        this.saslAuthenticator = saslAuthenticator;
        this.consumer = new ConfigTopicConsumer(bootstrapServers, topic, groupId, extraProps, this::apply);
    }

    /// Applies a snapshot to the three mutable consumers. Package-private so the wiring is
    /// testable without a broker.
    void apply(GatewayConfig config) {
        authorizer.reload(config.rbac()); // risky first: can throw on unknown role
        virtualTopics.reload(config.virtualTopics());
        saslAuthenticator.reload(config.auth().mechanisms(), config.auth().users());
        lastConfig = config;
    }

    /// Starts the config-topic consumer. Idempotent.
    public void start() {
        consumer.start();
    }

    /// Blocks until the config topic has been caught up from the earliest offset.
    public void awaitInitialLoad() throws InterruptedException {
        consumer.awaitInitialLoad();
    }

    /// Whether the initial catch-up has completed.
    public boolean initialLoadComplete() {
        return consumer.initialLoadComplete();
    }

    /// The most recently applied config snapshot, or `null` before the first successful
    /// application.
    public GatewayConfig lastConfig() {
        return lastConfig;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
