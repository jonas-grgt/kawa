package io.jonasg.kawa.config;

/// Observability configuration.
///
/// @param enabled whether metrics are collected and exposed
/// @param prometheusPort optional port for a Prometheus text-format endpoint (`null` disables it)
public record MetricsConfig(boolean enabled, Integer prometheusPort) {

    public MetricsConfig {
        if (prometheusPort == null) {
            prometheusPort = 0;
        }
    }

    public static MetricsConfig of(
            boolean enabled,
            Integer prometheusPort
    ) {
        return new MetricsConfig(enabled, prometheusPort);
    }
}
