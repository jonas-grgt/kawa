package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceExemptionConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import io.jonasg.kawa.governance.GovernancePolicy;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class GovernanceConfigHandlerTest {

    private static final GovernancePolicy POLICY = new GovernancePolicy(new GovernanceConfig(null, null));

    @Test
    void getListsConfiguredGovernance() {
        // given
        var governance = new GovernanceConfig(
                Map.of("min-replication",
                        new GovernanceRuleConfig("replication factor must be at least 3", "topic.replicationFactor >= 3")),
                Map.of("ops", new GovernanceExemptionConfig(".*", ".*-changelog")));
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty().updateGovernance(governance));
        var handler = new GovernanceConfigHandler(repository, POLICY);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("GET", "/config/governance", Map.of(), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.body()).isEqualTo(governance);
    }

    @Test
    void getReturnsEmptyGovernanceWhenNoSnapshotApplied() {
        // given
        var handler = new GovernanceConfigHandler(new FakeGatewayConfigRepository(null), POLICY);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("GET", "/config/governance", Map.of(), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.body()).isEqualTo(new GovernanceConfig(null, null));
    }

    @Test
    void putReplacesGovernanceAndPersistsSnapshot() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty());
        var handler = new GovernanceConfigHandler(repository, POLICY);
        byte[] body = ("{\"topicRules\":{\"min-replication\":{\"message\":\"replication factor must be at least 3\","
                + "\"expression\":\"topic.replicationFactor >= 3\"}},"
                + "\"exemptions\":{\"ops\":{\"principal\":\".*\",\"topicPattern\":\".*-changelog\"}}}")
                .getBytes(StandardCharsets.UTF_8);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/config/governance", Map.of(), body));

        // then
        assertThat(response.status()).isEqualTo(200);
        assertThat(repository.current().governance().topicRules()).containsKey("min-replication");
        assertThat(repository.current().governance().exemptions()).containsKey("ops");
    }

    @Test
    void putRejectsInvalidRuleExpression() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty());
        var handler = new GovernanceConfigHandler(repository, POLICY);
        byte[] body = ("{\"topicRules\":{\"broken\":{\"message\":\"must be valid\","
                + "\"expression\":\"topic.replicationFactor >=\"}}}").getBytes(StandardCharsets.UTF_8);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/config/governance", Map.of(), body));

        // then
        assertThat(response.status()).isEqualTo(400);
        assertThat(repository.current().governance().topicRules()).isEmpty();
    }

    @Test
    void putRejectsInvalidBody() {
        // given
        var handler = new GovernanceConfigHandler(new FakeGatewayConfigRepository(GatewayConfig.empty()), POLICY);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/config/governance", Map.of(),
                        "not json".getBytes(StandardCharsets.UTF_8)));

        // then
        assertThat(response.status()).isEqualTo(400);
    }
}