package io.jonasg.kawa.virtualtopic;

import io.jonasg.kawa.core.GatewayContext;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class VirtualTopicState {

    private final Map<String, String> physicalToVirtual = new LinkedHashMap<>();
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
            String virtual
    ) {
        physicalToVirtual.put(physical, virtual);
    }

    public String virtualFor(String physical) {
        return physicalToVirtual.get(physical);
    }

    public Map<String, String> physicalToVirtual() {
        return Map.copyOf(physicalToVirtual);
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
