package io.jonasg.kawa.core.cluster;

public record BrokerNode(int id, String host, int port, String rack) {

    public static BrokerNode of(
            int id,
            String host,
            int port,
            String rack
    ) {
        return new BrokerNode(id, host, port, rack);
    }
}
