package io.jonasg.kawa.server.broker;

import io.jonasg.kawa.core.GatewayContext;
import io.jonasg.kawa.protocol.kafka.KafkaHeader;
import io.jonasg.kawa.server.netty.ClientSession;

import java.util.concurrent.ScheduledFuture;

/// A request forwarded to a broker whose response is still expected.
///
/// @param session client connection to write the response back to
/// @param context the request's gateway context (metrics, route, timings)
/// @param requestHeader the client's original header (api key, version, correlation id)
/// @param brokerCorrelationId correlation id used on the broker connection
/// @param timeoutTask scheduled timeout; cancelled when the response arrives
public record PendingRequest(
        ClientSession session,
        GatewayContext context,
        KafkaHeader requestHeader,
        int brokerCorrelationId,
        ScheduledFuture<?> timeoutTask) {

    public short apiKey() {
        return requestHeader.apiKey();
    }

    public short apiVersion() {
        return requestHeader.apiVersion();
    }

    public String apiName() {
        return requestHeader.apiName();
    }

    public short responseHeaderVersion() {
        return requestHeader.responseHeaderVersion();
    }
}
