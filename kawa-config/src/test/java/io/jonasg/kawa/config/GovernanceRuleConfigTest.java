package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GovernanceRuleConfigTest {

    @Test
    void rejectsNullMessage() {
        assertThatThrownBy(() -> new GovernanceRuleConfig(null, "true"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("message");
    }

    @Test
    void rejectsBlankMessage() {
        assertThatThrownBy(() -> new GovernanceRuleConfig("   ", "true"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("message");
    }

    @Test
    void rejectsNullExpression() {
        assertThatThrownBy(() -> new GovernanceRuleConfig("must be valid", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expression");
    }

    @Test
    void rejectsBlankExpression() {
        assertThatThrownBy(() -> new GovernanceRuleConfig("must be valid", "  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expression");
    }

    @Test
    void acceptsValidConfig() {
        // when
        GovernanceRuleConfig config = new GovernanceRuleConfig("must be valid", "true");

        // then
        assertThat(config.message()).isEqualTo("must be valid");
        assertThat(config.expression()).isEqualTo("true");
    }
}