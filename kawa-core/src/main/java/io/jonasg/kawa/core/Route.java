package io.jonasg.kawa.core;

/// Routing decision: the physical broker id a request should be forwarded to.
public record Route(int brokerId) {

    public static final Route UNROUTED = new Route(-1);

    public static Route to(int brokerId) {
        return new Route(brokerId);
    }
}
