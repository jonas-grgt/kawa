package io.jonasg.kawa.config;

/// Admin HTTP listener configuration for the gateway's management/UI surface.
///
/// @param enabled whether the admin HTTP server is started (`false` disables it)
/// @param host bind address (defaults to `0.0.0.0`)
/// @param port bind port, or `0` to bind an ephemeral port (defaults to `8080`)
public record AdminConfig(boolean enabled, String host, Integer port) {

    public AdminConfig {
        if (host == null) {
            host = "0.0.0.0";
        }
        if (port == null) {
            port = 8080;
        }
    }

    public static AdminConfig of(
            boolean enabled,
            String host,
            Integer port
    ) {
        return new AdminConfig(enabled, host, port);
    }
}
