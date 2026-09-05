package io.jonasg.kawa.http;

/// A single topic entry in the admin `/topics` response.
///
/// @param logicalName client-visible name, or `null` if the physical topic is not virtualized
/// @param physicalName the physical topic name on the broker
/// @param partitionCount number of partitions (from the live metadata cache)
/// @param replicationFactor number of replicas per partition (from the live metadata cache)
/// @param filter human-readable consume filter description, or `null` if none
/// @param exposePhysicalTopic whether the physical name is also listed alongside the logical name
public record TopicView(
        String logicalName,
        String physicalName,
        int partitionCount,
        int replicationFactor,
        String filter,
        boolean exposePhysicalTopic) {
}
