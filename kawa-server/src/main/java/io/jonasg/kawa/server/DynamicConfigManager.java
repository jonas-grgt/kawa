package io.jonasg.kawa.server;

import io.jonasg.kawa.config.ConfigTopicConsumer;
import io.jonasg.kawa.config.ConfigTopicRepository;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.core.VirtualTopicManager;
import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.rbac.RbacAuthorizer;
import io.jonasg.kawa.server.auth.SaslAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.function.UnaryOperator;

/// Owns the config-topic consumer and applies each [GatewayConfig] snapshot to the mutable
/// consumers: [VirtualTopicManager], [RbacAuthorizer], [SaslAuthenticator] and
/// [GovernancePolicy]. Also owns the config-topic producer, so it is the
/// [GatewayConfigRepository] the admin HTTP surface uses to read the current snapshot and
/// persist changes.
///
/// A snapshot is applied in an order that keeps the application atomic in the failure case:
/// [RbacAuthorizer#reload] and [GovernancePolicy#reload] are the consumers that can reject a
/// snapshot (an unknown role reference or an invalid CEL expression throws), so they run
/// first. Each of them is atomic on its own - a failed reload keeps the previous state - and
/// the non-risky consumers ([VirtualTopicManager], [SaslAuthenticator]) are only touched
/// after both risky reloads have succeeded.
///
/// [awaitInitialLoad] blocks until the config topic has been caught up from the earliest
/// offset, so the gateway can refuse to serve until it has applied the config that existed
/// at boot.
///
/// [current] reports the newest *persisted* snapshot (from the write repository) when one
/// exists, falling back to the last *applied* one. The admin API's read-modify-write must
/// build on the persisted snapshot: the consumer applies asynchronously, so a burst of PUTs
/// would otherwise each start from the same stale base and overwrite each other.
public final class DynamicConfigManager implements GatewayConfigRepository, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DynamicConfigManager.class);

    private final ConfigTopicConsumer consumer;
    private final GatewayConfigRepository writeRepository;
    private final VirtualTopicManager virtualTopics;
    private final RbacAuthorizer authorizer;
    private final SaslAuthenticator saslAuthenticator;
    private final GovernancePolicy governance;

    private volatile GatewayConfig current;

    public DynamicConfigManager(
            String bootstrapServers,
            String topic,
            String groupId,
            VirtualTopicManager virtualTopics,
            RbacAuthorizer authorizer,
            SaslAuthenticator saslAuthenticator,
            GovernancePolicy governance
    ) {
        this(bootstrapServers, topic, groupId, new Properties(), virtualTopics, authorizer, saslAuthenticator,
                governance);
    }

    /// Variant that accepts extra consumer/producer properties (e.g. SASL/security settings
    /// for the config topic) on top of the base bootstrap/deserializer configuration.
    public DynamicConfigManager(
            String bootstrapServers,
            String topic,
            String groupId,
            Properties extraProps,
            VirtualTopicManager virtualTopics,
            RbacAuthorizer authorizer,
            SaslAuthenticator saslAuthenticator,
            GovernancePolicy governance
    ) {
        this.virtualTopics = virtualTopics;
        this.authorizer = authorizer;
        this.saslAuthenticator = saslAuthenticator;
        this.governance = governance;
        this.consumer = new ConfigTopicConsumer(bootstrapServers, topic, groupId, extraProps, this::apply);
        this.writeRepository = new ConfigTopicRepository(bootstrapServers, topic, extraProps);
    }

    /// Test seam: injects the write-side repository so the read-modify-write base can be
    /// verified without a broker. The consumer is created against a dummy bootstrap and never
    /// started.
    DynamicConfigManager(
            GatewayConfigRepository writeRepository,
            VirtualTopicManager virtualTopics,
            RbacAuthorizer authorizer,
            SaslAuthenticator saslAuthenticator,
            GovernancePolicy governance
    ) {
        this.virtualTopics = virtualTopics;
        this.authorizer = authorizer;
        this.saslAuthenticator = saslAuthenticator;
        this.governance = governance;
        this.consumer = new ConfigTopicConsumer("localhost:9092", "__kawa", "test-group", this::apply);
        this.writeRepository = writeRepository;
    }

    /// Applies a snapshot to the four mutable consumers. Package-private so the wiring is
    /// testable without a broker.
    void apply(GatewayConfig config) {
        authorizer.reload(config.rbac()); // risky first: can throw on unknown role
        governance.reload(config.governance()); // risky: can throw on invalid CEL expression
        virtualTopics.reload(config.virtualTopics());
        saslAuthenticator.reload(config.auth().mechanisms(), config.auth().users());
        current = config;
    }

    /// Starts the config-topic consumer. Idempotent.
    public void start() {
        consumer.start();
    }

    /// Blocks until the config topic has been caught up from the earliest offset.
    public void awaitInitialLoad() throws InterruptedException {
        consumer.awaitInitialLoad();
    }

    @Override
    public GatewayConfig getActiveConfig() {
        // The admin API's read-modify-write must build on the most recent *persisted*
        // snapshot, not the last *applied* one: the consumer applies asynchronously, so a
        // burst of PUTs would otherwise each start from the same stale base and overwrite
        // each other. The write repository tracks the newest persisted snapshot.
        GatewayConfig persisted = writeRepository.getActiveConfig();
        return persisted != null ? persisted : current;
    }

    @Override
    public void upsert(GatewayConfig config) {
        writeRepository.upsert(config);
    }

    /// Applies [mutation] to the active config (persisted-or-applied, or empty before the
    /// first snapshot) and persists the result.
    @Override
    public void update(UnaryOperator<GatewayConfig> mutation) {
        upsert(mutation.apply(getActiveConfigOrEmpty()));
    }

    @Override
    public void close() {
        consumer.close();
        try {
            writeRepository.close();
        } catch (Exception e) {
            log.warn("failed to close config write repository", e);
        }
    }
}
