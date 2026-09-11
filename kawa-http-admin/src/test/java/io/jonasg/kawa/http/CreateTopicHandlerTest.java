package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceExemptionConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.governance.TopicSpec;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CreateTopicHandlerTest {

    private static final GovernancePolicy POLICY = new GovernancePolicy(new GovernanceConfig(null, null));

    private static final byte[] PHYSICAL_BODY = ("{\"type\":\"physical\",\"name\":\"orders\",\"partitions\":3,"
            + "\"replicationFactor\":3,\"configs\":{\"cleanup.policy\":\"compact\"}}")
            .getBytes(StandardCharsets.UTF_8);

    @Test
    void postCreatesPhysicalTopicOnBroker() {
        // given
        var topicAdmin = new FakeTopicAdmin();
        var handler = new CreateTopicHandler(POLICY, new FakeGatewayConfigRepository(null), topicAdmin);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("POST", "/topics", Map.of(), PHYSICAL_BODY));

        // then
        assertThat(response.status()).isEqualTo(201);
        assertThat(response.body()).isEqualTo(new TopicSpec("orders", 3, 3, Map.of("cleanup.policy", "compact")));
        assertThat(topicAdmin.created).containsExactly(new TopicSpec("orders", 3, 3, Map.of("cleanup.policy", "compact")));
    }

    @Test
    void postRejectsInvalidBody() {
        // given
        var handler = new CreateTopicHandler(POLICY, new FakeGatewayConfigRepository(null), new FakeTopicAdmin());

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("POST", "/topics", Map.of(), "not json".getBytes(StandardCharsets.UTF_8)));

        // then
        assertThat(response.status()).isEqualTo(400);
    }

    @Test
    void postRejectsUnknownType() {
        // given
        var handler = new CreateTopicHandler(POLICY, new FakeGatewayConfigRepository(null), new FakeTopicAdmin());

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("POST", "/topics", Map.of(),
                        "{\"type\":\"wormhole\",\"name\":\"orders\"}".getBytes(StandardCharsets.UTF_8)));

        // then
        assertThat(response.status()).isEqualTo(400);
    }

    @Test
    void postRejectsMissingName() {
        // given
        var handler = new CreateTopicHandler(POLICY, new FakeGatewayConfigRepository(null), new FakeTopicAdmin());

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("POST", "/topics", Map.of(),
                        "{\"type\":\"physical\",\"partitions\":3}".getBytes(StandardCharsets.UTF_8)));

        // then
        assertThat(response.status()).isEqualTo(400);
    }

    @Test
    void postRejectsTopicViolatingRules() {
        // given
        var governance = new GovernanceConfig(
                Map.of("min-replication",
                        new GovernanceRuleConfig("replication factor must be at least 3", "topic.replicationFactor >= 3")),
                Map.of());
        var topicAdmin = new FakeTopicAdmin();
        var handler = new CreateTopicHandler(new GovernancePolicy(governance),
                new FakeGatewayConfigRepository(null), topicAdmin);
        byte[] body = "{\"type\":\"physical\",\"name\":\"orders\",\"partitions\":3,\"replicationFactor\":1}"
                .getBytes(StandardCharsets.UTF_8);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("POST", "/topics", Map.of(), body));

        // then
        assertThat(response.status()).isEqualTo(403);
        assertThat(response.body()).isEqualTo(Map.of("error",
                "topic 'orders' rejected by governance: [min-replication] replication factor must be at least 3"));
        assertThat(topicAdmin.created).isEmpty();
    }

    @Test
    void postAcceptsExemptTopicAndCreatesOnBroker() {
        // given
        var governance = new GovernanceConfig(
                Map.of("min-replication",
                        new GovernanceRuleConfig("replication factor must be at least 3", "topic.replicationFactor >= 3")),
                Map.of("ops", new GovernanceExemptionConfig("admin", ".*")));
        var topicAdmin = new FakeTopicAdmin();
        var handler = new CreateTopicHandler(new GovernancePolicy(governance),
                new FakeGatewayConfigRepository(null), topicAdmin);
        byte[] body = "{\"type\":\"physical\",\"name\":\"orders\",\"partitions\":3,\"replicationFactor\":1}"
                .getBytes(StandardCharsets.UTF_8);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("POST", "/topics", Map.of(), body));

        // then
        assertThat(response.status()).isEqualTo(201);
        assertThat(topicAdmin.created).hasSize(1);
    }

    @Test
    void postReturnsConflictWhenTopicExists() {
        // given
        var topicAdmin = new FakeTopicAdmin();
        topicAdmin.createError = new TopicExistsException("Topic 'orders' already exists.");
        var handler = new CreateTopicHandler(POLICY, new FakeGatewayConfigRepository(null), topicAdmin);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("POST", "/topics", Map.of(), PHYSICAL_BODY));

        // then
        assertThat(response.status()).isEqualTo(409);
    }

    @Test
    void postCreatesVirtualTopicConfig() {
        // given
        var repository = new FakeGatewayConfigRepository(null);
        var topicAdmin = new FakeTopicAdmin();
        var handler = new CreateTopicHandler(POLICY, repository, topicAdmin);
        byte[] body = "{\"type\":\"virtual\",\"name\":\"orders\",\"topic\":\"orders-v2\"}"
                .getBytes(StandardCharsets.UTF_8);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("POST", "/topics", Map.of(), body));

        // then
        assertThat(response.status()).isEqualTo(201);
        assertThat(response.body()).isEqualTo(new VirtualTopicConfig("orders-v2"));
        assertThat(repository.current().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v2"));
        assertThat(topicAdmin.created).isEmpty();
    }

    @Test
    void postRejectsVirtualTopicWithoutBacking() {
        // given
        var handler = new CreateTopicHandler(POLICY, new FakeGatewayConfigRepository(null), new FakeTopicAdmin());

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("POST", "/topics", Map.of(),
                        "{\"type\":\"virtual\",\"name\":\"orders\"}".getBytes(StandardCharsets.UTF_8)));

        // then
        assertThat(response.status()).isEqualTo(400);
    }
}