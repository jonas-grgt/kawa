package io.jonasg.kawa.virtualtopic.filter;

import org.apache.kafka.common.record.internal.Record;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/// UTF-8 decoded values of a record's headers with a given key, in record order.
/// Headers with a null value are skipped. A record may carry multiple headers with the
/// same key, so all values are returned and predicates match when any of them passes.
final class HeaderValues {

    private HeaderValues() {
    }

    static java.util.List<String> of(Record record, String key) {
        return Arrays.stream(record.headers())
                .filter(header -> key.equals(header.key()) && header.value() != null)
                .map(header -> new String(header.value(), StandardCharsets.UTF_8))
                .toList();
    }
}
