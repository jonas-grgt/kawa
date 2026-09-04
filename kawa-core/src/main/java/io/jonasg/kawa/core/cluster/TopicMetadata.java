package io.jonasg.kawa.core.cluster;

import java.util.List;

/// @param name physical topic name
/// @param partitions partition metadata
public record TopicMetadata(String name, List<PartitionMetadata> partitions) {

    public static TopicMetadata of(
            String name,
            List<PartitionMetadata> partitions
    ) {
        return new TopicMetadata(name, partitions);
    }
}
