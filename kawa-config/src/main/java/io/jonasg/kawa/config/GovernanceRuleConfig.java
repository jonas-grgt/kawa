package io.jonasg.kawa.config;

/// A single topic governance rule: a human-readable [message] explaining what went wrong,
/// and a CEL [expression] that evaluates to `true` when the topic is compliant.
///
/// @param message description shown to the user when this rule rejects a topic
/// @param expression CEL expression evaluated against the topic bindings — must return boolean
public record GovernanceRuleConfig(String message, String expression) {

    public GovernanceRuleConfig {
        if (message == null || message.isBlank()) {
            throw new IllegalArgumentException("message must not be null or blank");
        }
        if (expression == null || expression.isBlank()) {
            throw new IllegalArgumentException("expression must not be null or blank");
        }
    }
}