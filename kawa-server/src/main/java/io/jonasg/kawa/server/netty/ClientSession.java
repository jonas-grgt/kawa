package io.jonasg.kawa.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/// Per-client-connection state: the channel plus the count of in-flight requests, used to
/// apply read backpressure when a client queues too many requests ahead of the brokers.
public final class ClientSession {

    private static final int HIGH_WATERMARK = 200;
    private static final int LOW_WATERMARK = 100;

    private final Channel channel;
    private final AtomicInteger inFlight = new AtomicInteger();
    private final Queue<Integer> requestOrder = new ArrayDeque<>();
    private final Set<Integer> openRequests = new HashSet<>();
    private final Map<Integer, ByteBuf> pendingResponses = new HashMap<>();
    private String principal;

    public ClientSession(Channel channel) {
        this.channel = channel;
    }

    public Channel channel() {
        return channel;
    }

    public void requestInFlight() {
        if (inFlight.incrementAndGet() >= HIGH_WATERMARK) {
            channel.config().setAutoRead(false);
        }
    }

    public void requestCompleted() {
        int remaining = inFlight.decrementAndGet();
        if (remaining <= LOW_WATERMARK) {
            channel.config().setAutoRead(true);
        }
    }

    public synchronized void requestReceived(int correlationId) {
        requestOrder.add(correlationId);
        openRequests.add(correlationId);
    }

    public synchronized void close() {
        pendingResponses.values().forEach(ByteBuf::release);
        pendingResponses.clear();
        requestOrder.clear();
        openRequests.clear();
        channel.close();
    }

    public void write(ByteBuf frame) {
        channel.writeAndFlush(frame);
    }

    public synchronized void writeResponse(
            int correlationId,
            ByteBuf frame
    ) {
        if (!openRequests.contains(correlationId)) {
            channel.writeAndFlush(frame);
            return;
        }
        pendingResponses.put(correlationId, frame);
        flushReadyResponses();
    }

    private void flushReadyResponses() {
        while (true) {
            Integer nextCorrelationId = requestOrder.peek();
            if (nextCorrelationId == null) {
                return;
            }
            ByteBuf next = pendingResponses.remove(nextCorrelationId);
            if (next == null) {
                return;
            }
            requestOrder.remove();
            openRequests.remove(nextCorrelationId);
            channel.writeAndFlush(next);
        }
    }

	public void setPrincipal(String principal) {
        this.principal = principal;
	}

    public String principal() {
        return principal;
    }
}
