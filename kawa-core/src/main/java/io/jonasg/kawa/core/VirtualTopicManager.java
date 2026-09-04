package io.jonasg.kawa.core;

import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.config.VirtualTopicFilterConfig;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/// Maintains the logical-to-physical virtual-topic map, along with each virtual topic's
/// optional consume filter configuration. Immutable once constructed.
///
/// Topic names that are not virtualized map to themselves (identity) and carry no filter.
public final class VirtualTopicManager {

    private record Entry(String logical, String physical, VirtualTopicFilterConfig filter, boolean exposePhysicalTopic) {
    }

    private final Map<String, Entry> byLogical;
    private final Map<String, Entry> byPhysical;
    private final Map<String, String> logicalToPhysical;

    public VirtualTopicManager(Map<String, VirtualTopicConfig> virtualTopics) {
        Map<String, Entry> logical = new LinkedHashMap<>();
        Map<String, Entry> physical = new LinkedHashMap<>();
        Map<String, String> names = new LinkedHashMap<>();
        virtualTopics.forEach((l, config) -> {
            Entry entry = new Entry(l, config.topic(), config.filter(), config.exposePhysicalTopic());
            logical.put(l, entry);
            physical.put(config.topic(), entry);
            names.put(l, config.topic());
        });
        this.byLogical = Collections.unmodifiableMap(logical);
        this.byPhysical = Collections.unmodifiableMap(physical);
        this.logicalToPhysical = Collections.unmodifiableMap(names);
    }

    /// Maps a client-visible name to the physical topic name the broker must see.
    public String toPhysical(String logical) {
        Entry entry = byLogical.get(logical);
        return entry == null ? logical : entry.physical();
    }

    /// Maps a physical topic name back to the client-visible (logical) name.
    public String toLogical(String physical) {
        Entry entry = byPhysical.get(physical);
        return entry == null ? physical : entry.logical();
    }

    public boolean hasVirtualTopic(String physical) {
        return byPhysical.containsKey(physical);
    }

    public int size() {
        return byLogical.size();
    }

    /// Logical-to-physical virtual-topic map (unmodifiable).
    public Map<String, String> virtualTopics() {
        return logicalToPhysical;
    }

    /// The configured consume filter for a virtual topic, looked up by either its logical or
    /// physical name, or [Optional#empty] if the topic isn't virtualized or has no
    /// filter configured.
    public Optional<VirtualTopicFilterConfig> filterFor(String logicalOrPhysical) {
        Entry entry = byLogical.get(logicalOrPhysical);
        if (entry == null) {
            entry = byPhysical.get(logicalOrPhysical);
        }
        return entry == null ? Optional.empty() : Optional.ofNullable(entry.filter());
    }

    /// Whether this virtual topic's physical name should still be listed alongside its logical
    /// name in Metadata responses. `false` (hidden - physical renamed to logical in place)
    /// for non-virtualized topics and for virtual topics that don't opt in.
    public boolean exposesPhysicalTopic(String logicalOrPhysical) {
        Entry entry = byLogical.get(logicalOrPhysical);
        if (entry == null) {
            entry = byPhysical.get(logicalOrPhysical);
        }
        return entry != null && entry.exposePhysicalTopic();
    }
}
