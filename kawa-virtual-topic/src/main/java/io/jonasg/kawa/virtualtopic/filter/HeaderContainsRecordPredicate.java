package io.jonasg.kawa.virtualtopic.filter;

import io.jonasg.kawa.config.HeaderContainsFilterConfig;
import org.apache.kafka.common.record.internal.Record;

public class HeaderContainsRecordPredicate implements RecordPredicate<HeaderContainsFilterConfig> {

	@Override
	public boolean test(HeaderContainsFilterConfig config, Record record) {
		return HeaderValues.of(record, config.header()).stream()
				.anyMatch(value -> value.contains(config.value()));
	}
}
