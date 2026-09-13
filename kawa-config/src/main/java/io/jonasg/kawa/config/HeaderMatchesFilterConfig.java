package io.jonasg.kawa.config;

import java.util.regex.Pattern;

/// Filter that keeps only records with a `header` record header whose value fully matches the
/// `value` regular expression (anchored, like `Pattern#matches`).
///
/// The regex is validated at config load time so an invalid pattern fails fast.
///
/// @param header header key to compare
/// @param value regular expression the header value must fully match
public record HeaderMatchesFilterConfig(
        String header,
        String value
) implements VirtualTopicFilterConfig {

    public HeaderMatchesFilterConfig {
        Pattern.compile(value);
    }
}