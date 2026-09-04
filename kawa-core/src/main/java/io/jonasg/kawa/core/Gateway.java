package io.jonasg.kawa.core;

public interface Gateway {

    /// Starts the gateway (binds listeners, connects to the cluster). Blocks until ready.
    void start() throws Exception;

    void stop();
}
