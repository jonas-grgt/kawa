package io.jonasg.kawa.protocol.kafka;

import java.util.HashMap;
import java.util.Map;

/// Version strategy: the ranges this gateway decodes/advertises, intersected with what the
/// broker actually supports.
public final class SupportedVersions {

    private final Map<Short, VersionRange> decodeRanges;

    public SupportedVersions(Map<Short, VersionRange> decodeRanges) {
        this.decodeRanges = Map.copyOf(decodeRanges);
    }

    public static SupportedVersions from(KafkaApiRegistry registry) {
        Map<Short, VersionRange> ranges = new HashMap<>();
        for (KafkaApiSpec spec : registry.specs()) {
            ranges.put(spec.apiKey(), spec.versionRange());
        }
        return new SupportedVersions(ranges);
    }

    /// The range the gateway decodes for an api key, or `null`.
    public VersionRange decodeRange(short apiKey) {
        return decodeRanges.get(apiKey);
    }

    /// The range to advertise to clients: the gateway's decode range intersected with what
    /// the broker supports, or the broker's range verbatim for passthrough APIs.
    public VersionRange advertised(
            short apiKey,
            VersionRange brokerRange
    ) {
        VersionRange mine = decodeRanges.get(apiKey);
        if (mine == null) {
            return brokerRange;
        }
        return mine.intersect(brokerRange);
    }

    public Map<Short, VersionRange> decodeRanges() {
        return decodeRanges;
    }
}
