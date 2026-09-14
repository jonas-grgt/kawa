package io.jonasg.kawa.config;

import java.util.function.UnaryOperator;

/// A [GatewayConfigRepository] that can report the Kafka offset for a persisted config write.
/// This allows callers to correlate write progress with consumer apply progress.
public interface OffsetAwareGatewayConfigRepository extends GatewayConfigRepository {

    /// Applies mutation and persists the resulting snapshot, returning the offset of the written
    /// config record.
    long updateAndGetOffset(UnaryOperator<GatewayConfig> mutation);
}
