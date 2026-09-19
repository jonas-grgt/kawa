package io.jonasg.kawa.virtualtopic.filter;

import io.jonasg.kawa.config.HeaderMatchesFilterConfig;
import org.apache.kafka.common.record.internal.Record;

import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class HeaderMatchesRecordPredicate implements RecordPredicate<HeaderMatchesFilterConfig> {

    /// Compiled patterns keyed by pattern string. The config record already validates the regex
    /// at load time, so the cache only ever sees valid patterns; it exists to keep the hot path
    /// free of per-record `Pattern.compile`.
    private final ConcurrentHashMap<String, Pattern> patterns = new ConcurrentHashMap<>();

    @Override
    public boolean test(HeaderMatchesFilterConfig config, Record record) {
        Pattern pattern = patterns.computeIfAbsent(config.value(), Pattern::compile);
        return HeaderValues.of(record, config.header()).stream()
                .anyMatch(value -> pattern.matcher(value).matches());
    }
}
