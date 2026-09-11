package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GovernanceExemptionConfigTest {

    @Test
    void rejectsNullPrincipal() {
        assertThatThrownBy(() -> new GovernanceExemptionConfig(null, ".*"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("principal");
    }

    @Test
    void rejectsBlankPrincipal() {
        assertThatThrownBy(() -> new GovernanceExemptionConfig("   ", ".*"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("principal");
    }

    @Test
    void rejectsNullTopicPattern() {
        assertThatThrownBy(() -> new GovernanceExemptionConfig("^streams-.*", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("topicPattern");
    }

    @Test
    void rejectsBlankTopicPattern() {
        assertThatThrownBy(() -> new GovernanceExemptionConfig("^streams-.*", "  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("topicPattern");
    }

    @Test
    void rejectsInvalidPrincipalRegex() {
        assertThatThrownBy(() -> new GovernanceExemptionConfig("[invalid", ".*"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsInvalidTopicPatternRegex() {
        assertThatThrownBy(() -> new GovernanceExemptionConfig("^streams-.*", "[invalid"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void acceptsValidConfig() {
        // when
        GovernanceExemptionConfig config = new GovernanceExemptionConfig("^streams-.*", ".*-changelog$");

        // then
        assertThat(config.principal()).isEqualTo("^streams-.*");
        assertThat(config.topicPattern()).isEqualTo(".*-changelog$");
    }
}