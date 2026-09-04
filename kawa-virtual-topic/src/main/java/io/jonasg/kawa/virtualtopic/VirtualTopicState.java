package io.jonasg.kawa.virtualtopic;

import io.jonasg.kawa.core.GatewayContext;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class VirtualTopicState {

    private final Map<String, String> physicalToLogical = new LinkedHashMap<>();
    private int fetchSessionId;
    private boolean offsetFetchAllTopics;
    private short apiVersion;
    private List<String> rejectedCreateTopics = List.of();
    private List<String> rejectedDeleteTopics = List.of();

    public static VirtualTopicState from(GatewayContext context) {
        VirtualTopicState existing = context.state(VirtualTopicState.class);
        if (existing == null) {
            existing = new VirtualTopicState();
            context.state(VirtualTopicState.class, existing);
        }
        return existing;
    }

    public void record(
            String physical,
            String logical
    ) {
        physicalToLogical.put(physical, logical);
    }

    public String logicalFor(String physical) {
        return physicalToLogical.get(physical);
    }

    public Map<String, String> physicalToLogical() {
        return Map.copyOf(physicalToLogical);
    }

    public int fetchSessionId() {
        return fetchSessionId;
    }

    public void fetchSessionId(int sessionId) {
        this.fetchSessionId = sessionId;
    }

    public boolean offsetFetchAllTopics() {
        return offsetFetchAllTopics;
    }

    public void offsetFetchAllTopics(boolean value) {
        this.offsetFetchAllTopics = value;
    }

    public short apiVersion() {
        return apiVersion;
    }

    public void apiVersion(short version) {
        this.apiVersion = version;
    }

    public List<String> rejectedCreateTopics() {
        return rejectedCreateTopics;
    }

    public void rejectedCreateTopics(List<String> rejected) {
        this.rejectedCreateTopics = rejected;
    }

    public List<String> rejectedDeleteTopics() {
        return rejectedDeleteTopics;
    }

    public void rejectedDeleteTopics(List<String> rejected) {
        this.rejectedDeleteTopics = rejected;
    }
}
