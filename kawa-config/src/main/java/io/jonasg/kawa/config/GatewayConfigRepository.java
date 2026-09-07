package io.jonasg.kawa.config;

/// Read/write access to the dynamic gateway config. [current] returns the most recently
/// persisted snapshot (falling back to the last applied one before any write); [write]
/// persists a full snapshot to the config topic, which the consumer applies asynchronously
/// through the normal flow.
public interface GatewayConfigRepository extends AutoCloseable {

    /// The most recent config snapshot, or `null` before any snapshot has been persisted or
    /// applied (an empty config topic on first boot).
    GatewayConfig current();

    /// Persists a full config snapshot to the config topic, blocking until the broker
    /// acknowledges. The change is applied asynchronously once the consumer picks it up.
    void write(GatewayConfig config);

    /// Releases the repository's resources (e.g. the config-topic producer).
    @Override
    void close();
}