package io.jonasg.kawa.config;

/// The endpoint the gateway advertises to clients in rewritten Metadata and
/// FindCoordinator responses, so that all client traffic is routed through the gateway.
///
/// @param nodeId broker node id the gateway advertises itself as
/// @param host host clients connect to (defaults to `localhost`)
/// @param port port clients connect to; `0` or `null` means "use the bound listener port"
public record AdvertisedListener(Integer nodeId, String host, Integer port) {

    public AdvertisedListener {
        if (nodeId == null) {
            nodeId = 1;
        }
        if (host == null) {
            host = "localhost";
        }
        if (port == null) {
            port = 0;
        }
    }

    public static AdvertisedListener of(
            Integer nodeId,
            String host,
            Integer port
    ) {
        return new AdvertisedListener(nodeId, host, port);
    }
}
