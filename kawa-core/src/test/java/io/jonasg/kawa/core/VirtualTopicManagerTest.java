package io.jonasg.kawa.core;

import io.jonasg.kawa.config.HeaderEqualsFilterConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VirtualTopicManagerTest {

    private final VirtualTopicManager virtualTopics = new VirtualTopicManager(Map.of(
            "orders", new VirtualTopicConfig("orders-v2"),
            "customers", new VirtualTopicConfig("crm.customers",
                    new HeaderEqualsFilterConfig("tenant", "acme")),
            "legacy", new VirtualTopicConfig("legacy-v1", null, true)));

    @Test
    void mapsLogicalToPhysical() {
        assertThat(virtualTopics.toPhysical("orders")).isEqualTo("orders-v2");
        assertThat(virtualTopics.toPhysical("customers")).isEqualTo("crm.customers");
    }

    @Test
    void mapsPhysicalToLogical() {
        assertThat(virtualTopics.toLogical("orders-v2")).isEqualTo("orders");
        assertThat(virtualTopics.toLogical("crm.customers")).isEqualTo("customers");
    }

    @Test
    void identityForNonVirtualTopics() {
        assertThat(virtualTopics.toPhysical("plain")).isEqualTo("plain");
        assertThat(virtualTopics.toLogical("plain")).isEqualTo("plain");
    }

    @Test
    void exposesImmutableVirtualTopics() {
        assertThatThrownBy(() -> virtualTopics.virtualTopics().put("a", "b"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void filterForReturnsConfiguredFilterByLogicalOrPhysicalName() {
        var expected = new HeaderEqualsFilterConfig("tenant", "acme");

        assertThat(virtualTopics.filterFor("customers")).contains(expected);
        assertThat(virtualTopics.filterFor("crm.customers")).contains(expected);
    }

    @Test
    void filterForIsEmptyWhenNoFilterConfigured() {
        assertThat(virtualTopics.filterFor("orders")).isEmpty();
        assertThat(virtualTopics.filterFor("orders-v2")).isEmpty();
    }

    @Test
    void filterForIsEmptyForNonVirtualTopics() {
        assertThat(virtualTopics.filterFor("plain")).isEmpty();
    }

    @Test
    void exposesPhysicalTopicIsFalseByDefault() {
        assertThat(virtualTopics.exposesPhysicalTopic("orders")).isFalse();
        assertThat(virtualTopics.exposesPhysicalTopic("orders-v2")).isFalse();
    }

    @Test
    void exposesPhysicalTopicWhenOptedIn() {
        assertThat(virtualTopics.exposesPhysicalTopic("legacy")).isTrue();
        assertThat(virtualTopics.exposesPhysicalTopic("legacy-v1")).isTrue();
    }

    @Test
    void exposesPhysicalTopicIsFalseForNonVirtualTopics() {
        assertThat(virtualTopics.exposesPhysicalTopic("plain")).isFalse();
    }
}
