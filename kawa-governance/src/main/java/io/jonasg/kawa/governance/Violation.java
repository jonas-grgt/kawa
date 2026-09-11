package io.jonasg.kawa.governance;

/// A single governance rule rejection: which rule rejected the topic and why.
///
/// @param rule the rule name
/// @param message the rule's human-readable message
public record Violation(String rule, String message) {
}