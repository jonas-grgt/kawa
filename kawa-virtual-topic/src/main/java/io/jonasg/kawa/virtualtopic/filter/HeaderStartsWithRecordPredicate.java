package io.jonasg.kawa.virtualtopic.filter;

import io.jonasg.kawa.config.HeaderStartsWithFilterConfig;
import org.apache.kafka.common.record.internal.Record;

public class HeaderStartsWithRecordPredicate implements RecordPredicate<HeaderStartsWithFilterConfig> {

	@Override
	public boolean test(HeaderStartsWithFilterConfig config, Record record) {
		return HeaderValues.of(record, config.header()).stream()
				.anyMatch(value -> value.startsWith(config.value()));
	}
}
