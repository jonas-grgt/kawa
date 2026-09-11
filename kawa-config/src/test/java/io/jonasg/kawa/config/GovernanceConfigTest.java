package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GovernanceConfigTest {

    @Test
    void nullTopicRulesAndExemptionsCoalesceToEmpty() {
        // when
        GovernanceConfig config = new GovernanceConfig(null, null);

        // then
        assertThat(config.topicRules()).isEmpty();
        assertThat(config.exemptions()).isEmpty();
    }

    @Test
    void copiesAreImmutable() {
        // given
        Map<String, GovernanceRuleConfig> rules = new HashMap<>(Map.of(
                "min-partitions", new GovernanceRuleConfig("must have partitions", "topic.partitions >= 1")));
        Map<String, GovernanceExemptionConfig> exemptions = new HashMap<>(Map.of(
                "streams-internal", new GovernanceExemptionConfig("^streams-.*", ".*-changelog$")));

        // when
        GovernanceConfig config = new GovernanceConfig(rules, exemptions);

        // then
        assertThatThrownBy(() -> config.topicRules().put("x", null))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> config.exemptions().put("x", null))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void withRuleAddsNewRule() {
        // given
        var config = new GovernanceConfig(null, null);

        // when
        var updated = config.withRule("min-partitions",
                new GovernanceRuleConfig("must have partitions", "topic.partitions >= 1"));

        // then
        assertThat(updated.topicRules()).containsEntry("min-partitions",
                new GovernanceRuleConfig("must have partitions", "topic.partitions >= 1"));
    }

    @Test
    void withRuleOverwritesExisting() {
        // given
        var config = new GovernanceConfig(Map.of("min-partitions",
                new GovernanceRuleConfig("old", "true")), null);
        var newRule = new GovernanceRuleConfig("must have partitions", "topic.partitions >= 1");

        // when
        var updated = config.withRule("min-partitions", newRule);

        // then
        assertThat(updated.topicRules()).containsEntry("min-partitions", newRule);
        assertThat(updated.topicRules()).hasSize(1);
    }

    @Test
    void withoutRuleRemovesExisting() {
        // given
        var config = new GovernanceConfig(Map.of("min-partitions",
                new GovernanceRuleConfig("must have partitions", "topic.partitions >= 1")), null);

        // when
        var updated = config.withoutRule("min-partitions");

        // then
        assertThat(updated.topicRules()).isEmpty();
    }

    @Test
    void withoutRulePreservesOtherRules() {
        // given
        var config = new GovernanceConfig(Map.of(
                "min-partitions", new GovernanceRuleConfig("must have partitions", "topic.partitions >= 1"),
                "max-partitions", new GovernanceRuleConfig("too many partitions", "topic.partitions <= 12")), null);

        // when
        var updated = config.withoutRule("min-partitions");

        // then
        assertThat(updated.topicRules()).hasSize(1);
        assertThat(updated.topicRules()).containsKey("max-partitions");
    }

    @Test
    void withExemptionAddsNewExemption() {
        // given
        var config = new GovernanceConfig(null, null);

        // when
        var updated = config.withExemption("streams-internal",
                new GovernanceExemptionConfig("^streams-.*", ".*-changelog$"));

        // then
        assertThat(updated.exemptions()).containsEntry("streams-internal",
                new GovernanceExemptionConfig("^streams-.*", ".*-changelog$"));
    }

    @Test
    void withExemptionOverwritesExisting() {
        // given
        var config = new GovernanceConfig(null, Map.of("streams-internal",
                new GovernanceExemptionConfig("^old-.*", ".*")));
        var newExemption = new GovernanceExemptionConfig("^streams-.*", ".*-changelog$");

        // when
        var updated = config.withExemption("streams-internal", newExemption);

        // then
        assertThat(updated.exemptions()).containsEntry("streams-internal", newExemption);
        assertThat(updated.exemptions()).hasSize(1);
    }

    @Test
    void withoutExemptionRemovesExisting() {
        // given
        var config = new GovernanceConfig(null, Map.of("streams-internal",
                new GovernanceExemptionConfig("^streams-.*", ".*-changelog$")));

        // when
        var updated = config.withoutExemption("streams-internal");

        // then
        assertThat(updated.exemptions()).isEmpty();
    }

    @Test
    void withoutExemptionPreservesOtherExemptions() {
        // given
        var config = new GovernanceConfig(null, Map.of(
                "streams-internal", new GovernanceExemptionConfig("^streams-.*", ".*-changelog$"),
                "mirror-maker", new GovernanceExemptionConfig("^mm2-.*", ".*")));

        // when
        var updated = config.withoutExemption("streams-internal");

        // then
        assertThat(updated.exemptions()).hasSize(1);
        assertThat(updated.exemptions()).containsKey("mirror-maker");
    }

    @Test
    void withRulePreservesExemptions() {
        // given
        var config = new GovernanceConfig(
                Map.of("min-partitions", new GovernanceRuleConfig("must have partitions", "topic.partitions >= 1")),
                Map.of("streams-internal", new GovernanceExemptionConfig("^streams-.*", ".*-changelog$")));

        // when
        var updated = config.withRule("max-partitions",
                new GovernanceRuleConfig("too many partitions", "topic.partitions <= 12"));

        // then
        assertThat(updated.topicRules()).hasSize(2);
        assertThat(updated.exemptions()).containsEntry("streams-internal",
                new GovernanceExemptionConfig("^streams-.*", ".*-changelog$"));
    }
}