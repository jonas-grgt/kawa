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

/// End-to-end flow for the unified `/topics` admin surface: physical topics are created and
/// deleted on the real broker, virtual topics are created/updated/removed as gateway config,
/// and `GET /topics` lists both.
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TopicsAdminApiIT {

    static final String CONFIG_TOPIC = "__kawa";

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    private static KafkaGateway gateway;
    private static AdminClient brokerAdmin;

    @BeforeAll
    void setUp() throws Exception {
        String brokerBootstrap = kafka.getBootstrapServers();
        brokerAdmin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, brokerBootstrap));
        brokerAdmin.createTopics(List.of(new NewTopic(CONFIG_TOPIC, 1, (short) 1)
                .configs(Map.of("cleanup.policy", "compact")))).all().get();

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
        if (brokerAdmin != null) {
            brokerAdmin.close();
        }
        if (gateway != null) {
            gateway.stop();
        }
    }

    @Test
    void createsAndDeletesPhysicalTopicViaAdminApi() throws Exception {
        // given - a fresh gateway with the admin HTTP server enabled
        String base = "http://127.0.0.1:" + gateway.adminBoundPort();
        var http = HttpClient.newHttpClient();

        // when - a physical topic is created through the unified /topics surface
        HttpResponse<String> create = http.send(
                HttpRequest.newBuilder(URI.create(base + "/topics"))
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"type\":\"physical\",\"name\":\"orders\",\"partitions\":3,\"replicationFactor\":1}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then - the topic exists on the broker
        assertThat(create.statusCode()).isEqualTo(201);
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() ->
                        assertThat(brokerAdmin.listTopics().names().get()).contains("orders"));

        // and - the gateway's metadata cache knows the topic before it can be deleted
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    HttpResponse<String> list = http.send(
                            HttpRequest.newBuilder(URI.create(base + "/topics")).GET().build(),
                            HttpResponse.BodyHandlers.ofString());
                    assertThat(list.body()).contains("\"type\":\"physical\"", "\"name\":\"orders\"");
                });

        // when - the topic is deleted through the unified /topics surface
        HttpResponse<String> delete = http.send(
                HttpRequest.newBuilder(URI.create(base + "/topics/orders"))
                        .DELETE()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then - the topic is gone from the broker
        assertThat(delete.statusCode()).isEqualTo(204);
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() ->
                        assertThat(brokerAdmin.listTopics().names().get()).doesNotContain("orders"));
    }

    @Test
    void createsUpdatesAndDeletesVirtualTopicViaAdminApi() throws Exception {
        // given - a fresh gateway with the admin HTTP server enabled
        String base = "http://127.0.0.1:" + gateway.adminBoundPort();
        var http = HttpClient.newHttpClient();

        // when - a virtual topic is created through the unified /topics surface
        HttpResponse<String> create = http.send(
                HttpRequest.newBuilder(URI.create(base + "/topics"))
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"type\":\"virtual\",\"name\":\"orders\",\"topic\":\"orders-v2\"}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then - the virtual topic is listed by GET /topics once the consumer applies it
        assertThat(create.statusCode()).isEqualTo(201);
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    HttpResponse<String> list = http.send(
                            HttpRequest.newBuilder(URI.create(base + "/topics")).GET().build(),
                            HttpResponse.BodyHandlers.ofString());
                    assertThat(list.body()).contains("\"type\":\"virtual\"", "\"name\":\"orders\"");
                });

        // when - the virtual topic config is updated
        HttpResponse<String> update = http.send(
                HttpRequest.newBuilder(URI.create(base + "/topics/orders"))
                        .PUT(HttpRequest.BodyPublishers.ofString("{\"type\":\"virtual\",\"topic\":\"orders-v3\"}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then - the update is persisted
        assertThat(update.statusCode()).isEqualTo(200);

        // when - the virtual topic is deleted
        HttpResponse<String> delete = http.send(
                HttpRequest.newBuilder(URI.create(base + "/topics/orders"))
                        .DELETE()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then - the virtual topic is no longer listed
        assertThat(delete.statusCode()).isEqualTo(204);
        HttpResponse<String> list = http.send(
                HttpRequest.newBuilder(URI.create(base + "/topics")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(list.body()).doesNotContain("\"name\":\"orders\"");
    }
}