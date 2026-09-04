package io.jonasg.kawa.protocol.kafka;

public record VersionRange(short min, short max) {

    public VersionRange {
        if (min < 0 || max < 0 || min > max) {
            throw new IllegalArgumentException("Invalid version range [" + min + ", " + max + "]");
        }
    }

    public boolean contains(short version) {
        return version >= min && version <= max;
    }

    /// Intersection with another range, or `null` if the ranges do not overlap.
    public VersionRange intersect(VersionRange other) {
        short lo = (short) Math.max(min, other.min);
        short hi = (short) Math.min(max, other.max);
        return lo <= hi ? new VersionRange(lo, hi) : null;
    }

    public static VersionRange of(
            int min,
            int max
    ) {
        return new VersionRange((short) min, (short) max);
    }
}
