package io.jonasg.kawa.config;

import java.util.HashMap;
import java.util.Map;

/// Topic governance configuration: named CEL rules that new topics must satisfy, and named
/// exemptions that skip evaluation for matching principal + topic pairs.
///
/// @param topicRules named rules, each a message plus a CEL expression
/// @param exemptions named exemptions, each a principal regex plus a topic pattern regex
public record GovernanceConfig(
        Map<String, GovernanceRuleConfig> topicRules,
        Map<String, GovernanceExemptionConfig> exemptions
) {

    public GovernanceConfig {
        topicRules = topicRules == null ? Map.of() : Map.copyOf(topicRules);
        exemptions = exemptions == null ? Map.of() : Map.copyOf(exemptions);
    }

    /// Returns a new [GovernanceConfig] with the given rule added or replaced.
    public GovernanceConfig withRule(String name, GovernanceRuleConfig rule) {
        var newRules = new HashMap<>(topicRules);
        newRules.put(name, rule);
        return new GovernanceConfig(newRules, exemptions);
    }

    /// Returns a new [GovernanceConfig] with the given rule removed.
    public GovernanceConfig withoutRule(String name) {
        var newRules = new HashMap<>(topicRules);
        newRules.remove(name);
        return new GovernanceConfig(newRules, exemptions);
    }

    /// Returns a new [GovernanceConfig] with the given exemption added or replaced.
    public GovernanceConfig withExemption(String name, GovernanceExemptionConfig exemption) {
        var newExemptions = new HashMap<>(exemptions);
        newExemptions.put(name, exemption);
        return new GovernanceConfig(topicRules, newExemptions);
    }

    /// Returns a new [GovernanceConfig] with the given exemption removed.
    public GovernanceConfig withoutExemption(String name) {
        var newExemptions = new HashMap<>(exemptions);
        newExemptions.remove(name);
        return new GovernanceConfig(topicRules, newExemptions);
    }
}