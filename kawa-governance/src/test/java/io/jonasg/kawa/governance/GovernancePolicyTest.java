package io.jonasg.kawa.governance;

import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceExemptionConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GovernancePolicyTest {

    @Test
    void reloadCompilesValidRulesEagerly() {
        // given
        var config = new GovernanceConfig(Map.of(
                "min-partitions", new GovernanceRuleConfig("must have partitions", "topic.partitions >= 1")), null);

        // when / then
        assertThatCode(() -> new GovernancePolicy(config)).doesNotThrowAnyException();
    }

    @Test
    void reloadRejectsInvalidExpression() {
        // given
        var config = new GovernanceConfig(Map.of(
                "broken", new GovernanceRuleConfig("broken rule", "topic.partitions >=")), null);

        // when / then
        assertThatThrownBy(() -> new GovernancePolicy(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("broken");
    }

    @Test
    void collectsAllViolations() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "min-partitions", new GovernanceRuleConfig("too few partitions", "topic.partitions >= 12"),
                "max-partitions", new GovernanceRuleConfig("too many partitions", "topic.partitions <= 3"),
                "cleanup", new GovernanceRuleConfig("must be compact", "topic.configs['cleanup.policy'] == 'compact'")), null));

        // when
        var violations = policy.evaluate("alice", "payments", new TopicSpec("orders", 6, 3, Map.of()));

        // then
        assertThat(violations).extracting(Violation::rule)
                .containsExactlyInAnyOrder("min-partitions", "max-partitions", "cleanup");
        assertThat(violations).extracting(Violation::message)
                .contains("too few partitions", "too many partitions", "must be compact");
    }

    @Test
    void sortsViolationsByRuleName() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "zeta", new GovernanceRuleConfig("z", "false"),
                "alpha", new GovernanceRuleConfig("a", "false"),
                "mid", new GovernanceRuleConfig("m", "false")), null));

        // when
        var violations = policy.evaluate("alice", "payments", new TopicSpec("orders", 6, 3, Map.of()));

        // then
        assertThat(violations).extracting(Violation::rule)
                .containsExactly("alpha", "mid", "zeta");
    }

    @Test
    void throwingRuleFailsClosed() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "broken", new GovernanceRuleConfig("broken rule", "topic.nonexistent == 'x'")), null));

        // when / then
        assertThatThrownBy(() -> policy.evaluate("alice", "payments", new TopicSpec("orders", 6, 3, Map.of())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("broken");
    }

    @Test
    void nonBooleanResultIsAViolation() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "returns-name", new GovernanceRuleConfig("must return boolean", "topic.name")), null));

        // when
        var violations = policy.evaluate("alice", "payments", new TopicSpec("orders", 6, 3, Map.of()));

        // then
        assertThat(violations).extracting(Violation::rule).containsExactly("returns-name");
    }

    @Test
    void emptyRulesProduceNoViolations() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(null, null));

        // when
        var violations = policy.evaluate("alice", "payments", new TopicSpec("orders", 6, 3, Map.of()));

        // then
        assertThat(violations).isEmpty();
    }

    @Test
    void brokerDefaultFieldsAreExposed() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "default-partitions", new GovernanceRuleConfig("partitions must be default", "topic.partitions == -1"),
                "default-replication", new GovernanceRuleConfig("replication must be default", "topic.replicationFactor == -1")), null));

        // when
        var violations = policy.evaluate("alice", "payments", new TopicSpec("orders", -1, -1, Map.of()));

        // then
        assertThat(violations).isEmpty();
    }

    @Test
    void exposesPrincipalAndServiceBindings() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "principal", new GovernanceRuleConfig("must be alice", "principal == 'alice'"),
                "service", new GovernanceRuleConfig("must be payments", "service == 'payments'")), null));

        // when
        var violations = policy.evaluate("bob", "payments", new TopicSpec("orders", 6, 3, Map.of()));

        // then
        assertThat(violations).extracting(Violation::rule).containsExactly("principal");
    }

    @Test
    void inOperatorWorksOnConfigs() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "has-cleanup", new GovernanceRuleConfig("must set cleanup.policy", "'cleanup.policy' in topic.configs")), null));

        // when
        var violations = policy.evaluate("alice", "payments",
                new TopicSpec("orders", 6, 3, Map.of("cleanup.policy", "compact")));

        // then
        assertThat(violations).isEmpty();
    }

    @Test
    void failedReloadLeavesPreviousSnapshotIntact() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(Map.of(
                "min-partitions", new GovernanceRuleConfig("must have partitions", "topic.partitions >= 1")), null));
        var broken = new GovernanceConfig(Map.of(
                "broken", new GovernanceRuleConfig("broken rule", "topic.partitions >=")), null);

        // when / then
        assertThatThrownBy(() -> policy.reload(broken))
                .isInstanceOf(IllegalArgumentException.class);

        // then - the old rules still evaluate
        assertThat(policy.evaluate("alice", "payments", new TopicSpec("orders", 6, 3, Map.of()))).isEmpty();
    }

    @Test
    void exemptsWhenBothPatternsMatch() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(null, Map.of(
                "streams-internal", new GovernanceExemptionConfig("^streams-.*", ".*-changelog$"))));

        // when / then
        assertThat(policy.exempt("streams-app", "orders-changelog")).isTrue();
    }

    @Test
    void nonMatchingPrincipalIsNotExempt() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(null, Map.of(
                "streams-internal", new GovernanceExemptionConfig("^streams-.*", ".*-changelog$"))));

        // when / then
        assertThat(policy.exempt("other-app", "orders-changelog")).isFalse();
    }

    @Test
    void nonMatchingTopicIsNotExempt() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(null, Map.of(
                "streams-internal", new GovernanceExemptionConfig("^streams-.*", ".*-changelog$"))));

        // when / then
        assertThat(policy.exempt("streams-app", "orders")).isFalse();
    }

    @Test
    void noExemptionsMeansNothingIsExempt() {
        // given
        var policy = new GovernancePolicy(new GovernanceConfig(null, null));

        // when / then
        assertThat(policy.exempt("streams-app", "orders-changelog")).isFalse();
    }

    @Test
    void validationErrorIsEmptyForValidExpression() {
        // when / then
        assertThat(GovernancePolicy.validationError("topic.partitions >= 1")).isEmpty();
    }

    @Test
    void validationErrorDescribesInvalidExpression() {
        // when / then
        assertThat(GovernancePolicy.validationError("topic.partitions >="))
                .isPresent()
                .hasValueSatisfying(message -> assertThat(message).contains("topic.partitions >="));
    }
}