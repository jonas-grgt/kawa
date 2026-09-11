package io.jonasg.kawa.config;

import java.util.regex.Pattern;

/// Exempts a principal + topic pair from governance evaluation. Both patterns must match for
/// the exemption to apply. Two patterns rather than a name-only one: a topic-only exemption
/// would be a bypass, because anyone could name a topic `...-changelog` to skip enforcement.
///
/// @param principal regex matched against the requesting principal
/// @param topicPattern regex matched against the topic name
public record GovernanceExemptionConfig(String principal, String topicPattern) {

    public GovernanceExemptionConfig {
        if (principal == null || principal.isBlank()) {
            throw new IllegalArgumentException("principal must not be null or blank");
        }
        if (topicPattern == null || topicPattern.isBlank()) {
            throw new IllegalArgumentException("topicPattern must not be null or blank");
        }
        Pattern.compile(principal);
        Pattern.compile(topicPattern);
    }
}