package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.config.AdminConfig;
import io.jonasg.kawa.config.AuthConfig;
import io.jonasg.kawa.config.ClusterConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.ListenerConfig;
import io.jonasg.kawa.config.MetricsConfig;
import io.jonasg.kawa.server.KafkaGateway;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end flow for topic governance: a fresh gateway with an empty config topic gets
/// governance rules written over HTTP, and topic creation through `POST /topics` is then
/// enforced against the live policy. Exercises the full loop: admin HTTP write -> config
/// topic -> consumer apply -> live governance reload -> admission check.
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GovernanceAdminApiIT {

    static final String CONFIG_TOPIC = "__kawa";

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    private static KafkaGateway gateway;

    @BeforeAll
    void setUp() throws Exception {
        String brokerBootstrap = kafka.getBootstrapServers();

        // Create the config topic but write nothing to it - the empty-topic first boot case.
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, brokerBootstrap))) {
            admin.createTopics(List.of(new NewTopic(CONFIG_TOPIC, 1, (short) 1)
                    .configs(Map.of("cleanup.policy", "compact")))).all().get();
        }

        var bootstrap = new GatewayConfig(
                "test-gateway",
                List.of(new ListenerConfig("127.0.0.1", 0)),
                Map.of("default", new ClusterConfig("default", List.of(brokerBootstrap))),
                null,
                new AdvertisedListener(1, "localhost", 0),
                new MetricsConfig(false, 0),
                new AuthConfig(null, null, null),
                null,
                new AdminConfig(true, "127.0.0.1", 0, null),
                CONFIG_TOPIC, null);

        gateway = new KafkaGateway(bootstrap);
        gateway.start();
    }

    @AfterAll
    void tearDown() {
        if (gateway != null) {
            gateway.stop();
        }
    }

    @Test
    void enforcesGovernanceRulesOnTopicCreation() throws Exception {
        // given - a fresh gateway with an empty config topic and the admin HTTP server enabled
        String base = "http://127.0.0.1:" + gateway.adminBoundPort();
        var http = HttpClient.newHttpClient();

        // when - governance rules and an exemption are written through the admin API
        HttpResponse<String> governancePut = http.send(
                HttpRequest.newBuilder(URI.create(base + "/governance"))
                        .PUT(HttpRequest.BodyPublishers.ofString("""
                                {"topicRules":{
                                  "min-partitions":{
                                    "message":"partitions must be at least 2",
                                    "expression":"topic.partitions >= 2"
                                  }
                                },
                                "exemptions":{
                                  "changelogs":{"principal":"admin","topicPattern":".*-changelog"}
                                }}
                                """))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then - the write is persisted and readable
        assertThat(governancePut.statusCode()).isEqualTo(200);
        HttpResponse<String> governanceGet = http.send(
                HttpRequest.newBuilder(URI.create(base + "/governance")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(governanceGet.statusCode()).isEqualTo(200);
        assertThat(governanceGet.body()).contains("\"min-partitions\"", "\"changelogs\"");

        // when - a topic violating the rule is requested (polling until the consumer applies the snapshot)
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    HttpResponse<String> rejected = http.send(
                            HttpRequest.newBuilder(URI.create(base + "/topics"))
                                    .POST(HttpRequest.BodyPublishers.ofString(
                                            "{\"type\":\"physical\",\"name\":\"orders\",\"partitions\":1,\"replicationFactor\":1}"))
                                    .build(),
                            HttpResponse.BodyHandlers.ofString());
                    assertThat(rejected.statusCode()).isEqualTo(403);
                    assertThat(rejected.body()).contains("partitions must be at least 2");
                });

        // then - an exempt topic is admitted despite violating the rule
        HttpResponse<String> exempt = http.send(
                HttpRequest.newBuilder(URI.create(base + "/topics"))
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"type\":\"physical\",\"name\":\"orders-changelog\",\"partitions\":3,\"replicationFactor\":1}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(exempt.statusCode()).isEqualTo(201);

        // and - a compliant topic is admitted
        HttpResponse<String> admitted = http.send(
                HttpRequest.newBuilder(URI.create(base + "/topics"))
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"type\":\"physical\",\"name\":\"orders\",\"partitions\":3,\"replicationFactor\":1}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(admitted.statusCode()).isEqualTo(201);
    }
}