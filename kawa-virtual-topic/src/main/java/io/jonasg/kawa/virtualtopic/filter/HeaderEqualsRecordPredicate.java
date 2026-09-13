package io.jonasg.kawa.virtualtopic.filter;

import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import org.apache.kafka.common.record.internal.Record;

public class HeaderEqualsRecordPredicate implements RecordPredicate<HeaderEqualsFilterConfig> {

	@Override
	public boolean test(HeaderEqualsFilterConfig config, Record record) {
		return HeaderValues.of(record, config.header()).stream()
				.anyMatch(value -> config.value().equals(value));
	}
}
