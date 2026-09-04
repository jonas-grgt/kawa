package io.jonasg.kawa.virtualtopic.filter;

import io.jonasg.kawa.config.CelFilterConfig;
import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.VirtualTopicFilterConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.internal.BaseRecords;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;

import java.nio.ByteBuffer;

/// Applies a virtual topic's configured consume filter to a fetched partition's records:
/// decodes the batch(es), drops records the filter rejects, and re-encodes a valid batch.
///
/// Built on Kafka's own [MemoryRecords#filterTo(TopicPartition, MemoryRecords.RecordFilter, ByteBuffer, int, BufferSupplier)], the same decode-filter-reencode
/// mechanism the broker's log cleaner (compaction) and client-side down-conversion use to drop
/// records from a batch and re-emit a valid one - rather than hand-rolling batch encoding.
/// Surviving records keep their original offsets (this is what makes offset gaps in a fetch
/// response safe for consumers, the same property compacted topics already rely on).
public final class VirtualTopicRecordFilter {

    public VirtualTopicRecordFilter() {
    }

    /// Returns `records` filtered per `filter`, or `records` unchanged when
    /// there is nothing to decode (null or empty - the fast path most partitions take).
    public BaseRecords apply(
            VirtualTopicFilterConfig filter,
            TopicPartition partition,
            BaseRecords records
    ) {
        if (!(records instanceof MemoryRecords memoryRecords) || memoryRecords.sizeInBytes() == 0) {
            return records;
        }

        ByteBuffer output = ByteBuffer.allocate(memoryRecords.sizeInBytes());
        memoryRecords.filterTo(
                new EvaluatingRecordFilter(filter),
                output,
                BufferSupplier.NO_CACHING);
        output.flip();
        return MemoryRecords.readableRecords(output);
    }

    /// Retains a record iff it matches the configured filter. Decides whether a single decoded
    /// [Record] matches a virtual topic's configured consume filter - pure evaluation over the
    /// sealed [VirtualTopicFilterConfig], no wire-encoding concerns. The switch below is
    /// exhaustive over the sealed interface's permitted subtypes: adding a new filter kind is a
    /// compile error here until a case is added.
    static final class EvaluatingRecordFilter extends MemoryRecords.RecordFilter {

        private final VirtualTopicFilterConfig filterCfg;

        EvaluatingRecordFilter(VirtualTopicFilterConfig filterCfg) {
            super(RecordBatch.NO_TIMESTAMP, -1L);
            this.filterCfg = filterCfg;
        }

        @Override
        protected BatchRetentionResult checkBatchRetention(RecordBatch batch) {
            // Always retain the batch, even if every record in it is filtered out: an empty
            // batch is still a valid batch, and retaining it preserves producer id/epoch and
            // sequence continuity for idempotent/transactional producers. Never DELETE here -
            // that would drop batch metadata a consumer may depend on.
            return new BatchRetentionResult(BatchRetention.RETAIN_EMPTY, false);
        }

        @Override
        protected boolean shouldRetainRecord(
                RecordBatch batch,
                Record record
        ) {
            return matches(record);
        }

        boolean matches(Record record) {
            return switch (filterCfg) {
                case HeaderEqualsFilterConfig headerEquals -> new HeaderEqualsRecordPredicate().test(headerEquals, record);
                case CelFilterConfig cfg -> new CelRecordPredicate().test(cfg, record);
            };
        }

    }
}
