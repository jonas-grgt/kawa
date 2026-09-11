package io.jonasg.kawa.governance;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicSpecTest {

    @Test
    void partitionsSpecifiedIsFalseForBrokerDefault() {
        // given
        var spec = new TopicSpec("orders", -1, 3, Map.of());

        // when / then
        assertThat(spec.partitionsSpecified()).isFalse();
    }

    @Test
    void partitionsSpecifiedIsTrueWhenSet() {
        // given
        var spec = new TopicSpec("orders", 6, 3, Map.of());

        // when / then
        assertThat(spec.partitionsSpecified()).isTrue();
    }

    @Test
    void replicationFactorSpecifiedIsFalseForBrokerDefault() {
        // given
        var spec = new TopicSpec("orders", 6, -1, Map.of());

        // when / then
        assertThat(spec.replicationFactorSpecified()).isFalse();
    }

    @Test
    void replicationFactorSpecifiedIsTrueWhenSet() {
        // given
        var spec = new TopicSpec("orders", 6, 3, Map.of());

        // when / then
        assertThat(spec.replicationFactorSpecified()).isTrue();
    }

    @Test
    void nullConfigsCoalesceToEmpty() {
        // when
        var spec = new TopicSpec("orders", 6, 3, null);

        // then
        assertThat(spec.configs()).isEmpty();
    }

    @Test
    void copiesAreImmutable() {
        // given
        Map<String, String> configs = new HashMap<>(Map.of("cleanup.policy", "compact"));

        // when
        var spec = new TopicSpec("orders", 6, 3, configs);

        // then
        assertThatThrownBy(() -> spec.configs().put("x", "y"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsBlankName() {
        assertThatThrownBy(() -> new TopicSpec("   ", 6, 3, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name");
    }
}