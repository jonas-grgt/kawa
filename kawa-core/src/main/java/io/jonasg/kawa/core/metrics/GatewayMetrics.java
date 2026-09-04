package io.jonasg.kawa.core.metrics;

import io.micrometer.core.instrument.MeterRegistry;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/// Micrometer facade for gateway observability. Instrumented by the transport layer
/// and by interceptors.
public final class GatewayMetrics {

    private final MeterRegistry registry;
    private final AtomicInteger activeClientConnections = new AtomicInteger();
    private final AtomicInteger activeBrokerConnections = new AtomicInteger();

    public GatewayMetrics(MeterRegistry registry) {
        this.registry = registry;
        registry.gauge("gateway.connections.client.active", activeClientConnections);
        registry.gauge("gateway.connections.broker.active", activeBrokerConnections);
    }

    public MeterRegistry registry() {
        return registry;
    }

    public void request(
            String api,
            String result
    ) {
        registry.counter("gateway.requests.total", "api", api, "result", result).increment();
    }

    public void response(
            String api,
            String result
    ) {
        registry.counter("gateway.responses.total", "api", api, "result", result).increment();
    }

    public void recordLatency(
            String api,
            long nanos
    ) {
        registry.timer("gateway.request.latency", "api", api).record(Duration.ofNanos(nanos));
    }

    public void bytesIn(long bytes) {
        registry.counter("gateway.bytes.in").increment(bytes);
    }

    public void bytesOut(long bytes) {
        registry.counter("gateway.bytes.out").increment(bytes);
    }

    public void clientConnectionOpened() {
        activeClientConnections.incrementAndGet();
    }

    public void clientConnectionClosed() {
        activeClientConnections.decrementAndGet();
    }

    public void brokerConnectionOpened() {
        activeBrokerConnections.incrementAndGet();
    }

    public void brokerConnectionClosed() {
        activeBrokerConnections.decrementAndGet();
    }

    public void virtualTopicHit(
            String direction,
            String logical,
            String physical
    ) {
        registry.counter("gateway.virtual_topic.hits", "direction", direction, "logical", logical, "physical", physical)
                .increment();
    }
}
