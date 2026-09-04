package io.jonasg.kawa.virtualtopic.filter;

import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.internal.Record;

import java.nio.charset.StandardCharsets;

public class HeaderEqualsRecordPredicate implements RecordPredicate<HeaderEqualsFilterConfig> {

	@Override
	public boolean test(HeaderEqualsFilterConfig config, Record record) {
		for (Header header : record.headers()) {
			if (config.header().equals(header.key())
					&& header.value() != null
					&& config.value().equals(new String(header.value(), StandardCharsets.UTF_8))) {
				return true;
			}
		}
		return false;
	}
}
