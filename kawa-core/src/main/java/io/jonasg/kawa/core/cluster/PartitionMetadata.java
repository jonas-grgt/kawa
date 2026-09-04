package io.jonasg.kawa.core.cluster;

import java.util.List;

/// @param index partition id
/// @param leaderId id of the broker currently leading the partition
/// @param replicas broker ids holding replicas
/// @param isr in-sync replica broker ids
/// @param offlineReplicas offline replica broker ids
public record PartitionMetadata(
        int index,
        int leaderId,
        List<Integer> replicas,
        List<Integer> isr,
        List<Integer> offlineReplicas) {

    public static PartitionMetadata of(
            int index,
            int leaderId,
            List<Integer> replicas,
            List<Integer> isr,
            List<Integer> offlineReplicas) {
        return new PartitionMetadata(index, leaderId, replicas, isr, offlineReplicas);
    }
}
