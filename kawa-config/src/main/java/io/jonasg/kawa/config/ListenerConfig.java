package io.jonasg.kawa.config;

/// A listener the gateway accepts client connections on.
///
/// @param host bind address (defaults to `0.0.0.0`)
/// @param port bind port, or `0` to bind an ephemeral port (defaults to `9092`)
public record ListenerConfig(String host, Integer port) {

    public ListenerConfig {
        if (host == null) {
            host = "0.0.0.0";
        }
        if (port == null) {
            port = 9092;
        }
    }

    public static ListenerConfig of(
            String host,
            Integer port
    ) {
        return new ListenerConfig(host, port);
    }
}
