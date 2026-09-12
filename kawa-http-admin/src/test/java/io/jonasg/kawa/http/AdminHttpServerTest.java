package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminConfig;
import io.jonasg.kawa.config.CorsConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.core.VirtualTopicManager;
import io.jonasg.kawa.core.cluster.BrokerNode;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.cluster.MetadataSnapshot;
import io.jonasg.kawa.core.cluster.PartitionMetadata;
import io.jonasg.kawa.core.cluster.TopicMetadata;
import io.jonasg.kawa.governance.GovernancePolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AdminHttpServerTest {

    private AdminHttpServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void servesTopicsOverHttp() throws Exception {
        // given
        var virtualTopics = new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2")));
        MetadataCache cache = new MetadataCache();
        cache.update(MetadataSnapshot.of(
                Map.of("orders-v2", TopicMetadata.of("orders-v2",
                        List.of(PartitionMetadata.of(0, 1, List.of(1), List.of(1), List.of())))),
                Map.of(1, BrokerNode.of(1, "localhost", 9092, null)),
                "test-cluster"));
        server = new AdminHttpServer(new AdminConfig(true, "127.0.0.1", 0, null), virtualTopics, cache,
                new FakeGatewayConfigRepository(GatewayConfig.empty()),
                new GovernancePolicy(new GovernanceConfig(null, null)), new FakeTopicAdmin());
        server.start();

        // when
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + "/topics"))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains(
                "\"type\":\"virtual\"",
                "\"name\":\"orders\"",
                "\"partitions\":1",
                "\"physicalTopic\":\"orders-v2\"",
                "\"type\":\"physical\"",
                "\"name\":\"orders-v2\"");
    }

    @Test
    void servesRenderedOpenApiDocsOverHttp() throws Exception {
        // given
        var virtualTopics = new VirtualTopicManager(Map.of());
        MetadataCache cache = new MetadataCache();
        server = new AdminHttpServer(new AdminConfig(true, "127.0.0.1", 0, null), virtualTopics, cache,
                new FakeGatewayConfigRepository(GatewayConfig.empty()),
                new GovernancePolicy(new GovernanceConfig(null, null)), new FakeTopicAdmin());
        server.start();

        // when
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + "/docs"))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Content-Type")).contains("text/html");
        assertThat(response.body()).contains("Kawa Admin API");
    }

    @Test
    void servesPreflightRequestWithCorsHeadersWhenConfigured() throws Exception {
        // given
        var virtualTopics = new VirtualTopicManager(Map.of());
        MetadataCache cache = new MetadataCache();
        var cors = new CorsConfig(List.of("http://localhost:8080"), null, null, null, null);
        server = new AdminHttpServer(new AdminConfig(true, "127.0.0.1", 0, cors), virtualTopics, cache,
                new FakeGatewayConfigRepository(GatewayConfig.empty()),
                new GovernancePolicy(new GovernanceConfig(null, null)), new FakeTopicAdmin());
        server.start();

        // when
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + "/topics"))
                        .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                        .header("Origin", "http://localhost:8080")
                        .header("Access-Control-Request-Method", "GET")
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Access-Control-Allow-Origin"))
                .contains("http://localhost:8080");
        assertThat(response.headers().firstValue("Access-Control-Allow-Methods"))
                .get()
                .asString()
                .contains("GET", "POST", "PUT", "DELETE", "OPTIONS");
    }

    @Test
    void addsCorsHeadersToActualResponseWhenConfigured() throws Exception {
        // given
        var virtualTopics = new VirtualTopicManager(Map.of());
        MetadataCache cache = new MetadataCache();
        var cors = new CorsConfig(List.of("http://localhost:8080"), null, null, null, null);
        server = new AdminHttpServer(new AdminConfig(true, "127.0.0.1", 0, cors), virtualTopics, cache,
                new FakeGatewayConfigRepository(GatewayConfig.empty()),
                new GovernancePolicy(new GovernanceConfig(null, null)), new FakeTopicAdmin());
        server.start();

        // when
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + "/topics"))
                        .GET()
                        .header("Origin", "http://localhost:8080")
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Access-Control-Allow-Origin"))
                .contains("http://localhost:8080");
    }

    @Test
    void doesNotAddCorsHeadersWhenNotConfigured() throws Exception {
        // given
        var virtualTopics = new VirtualTopicManager(Map.of());
        MetadataCache cache = new MetadataCache();
        server = new AdminHttpServer(new AdminConfig(true, "127.0.0.1", 0, null), virtualTopics, cache,
                new FakeGatewayConfigRepository(GatewayConfig.empty()),
                new GovernancePolicy(new GovernanceConfig(null, null)), new FakeTopicAdmin());
        server.start();

        // when
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + "/topics"))
                        .GET()
                        .header("Origin", "http://localhost:8080")
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Access-Control-Allow-Origin")).isEmpty();
    }

    @Test
    void configuresAuthUserOverHttp() throws Exception {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty());
        server = new AdminHttpServer(new AdminConfig(true, "127.0.0.1", 0, null),
                new VirtualTopicManager(Map.of()), new MetadataCache(), repository,
                new GovernancePolicy(new GovernanceConfig(null, null)), new FakeTopicAdmin());
        server.start();

        // when
        HttpResponse<String> put = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + "/auth/users/alice"))
                        .PUT(HttpRequest.BodyPublishers.ofString("{\"mechanism\":\"PLAIN\",\"password\":\"secret\"}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        HttpResponse<String> get = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + "/auth/users"))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then
        assertThat(put.statusCode()).isEqualTo(200);
        assertThat(get.statusCode()).isEqualTo(200);
        assertThat(get.body()).contains("\"username\":\"alice\"", "\"PLAIN\"");
        assertThat(repository.getActiveConfig().auth().users()).containsKey("alice");
    }

    @Test
    void servesGovernanceConfigOverHttp() throws Exception {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty());
        server = new AdminHttpServer(new AdminConfig(true, "127.0.0.1", 0, null),
                new VirtualTopicManager(Map.of()), new MetadataCache(), repository,
                new GovernancePolicy(new GovernanceConfig(null, null)), new FakeTopicAdmin());
        server.start();

        // when
        HttpResponse<String> put = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + "/governance"))
                        .PUT(HttpRequest.BodyPublishers.ofString(
                                "{\"topicRules\":{\"min-replication\":{\"message\":\"replication factor must be at least 3\","
                                        + "\"expression\":\"topic.replicationFactor >= 3\"}}}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        HttpResponse<String> get = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + "/governance"))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then
        assertThat(put.statusCode()).isEqualTo(200);
        assertThat(get.statusCode()).isEqualTo(200);
        assertThat(get.body()).contains("\"min-replication\"", "\"topic.replicationFactor >= 3\"");
        assertThat(repository.getActiveConfig().governance().topicRules()).containsKey("min-replication");
    }

    @Test
    void rejectsTopicOverHttpWhenGovernanceRuleViolated() throws Exception {
        // given
        var governance = new GovernanceConfig(
                Map.of("min-replication",
                        new GovernanceRuleConfig("replication factor must be at least 3", "topic.replicationFactor >= 3")),
                Map.of());
        server = new AdminHttpServer(new AdminConfig(true, "127.0.0.1", 0, null),
                new VirtualTopicManager(Map.of()), new MetadataCache(),
                new FakeGatewayConfigRepository(GatewayConfig.empty()),
                new GovernancePolicy(governance), new FakeTopicAdmin());
        server.start();

        // when
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + "/topics"))
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"type\":\"physical\",\"name\":\"orders\",\"partitions\":3,\"replicationFactor\":1}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then
        assertThat(response.statusCode()).isEqualTo(403);
        assertThat(response.body()).contains("replication factor must be at least 3");
    }

    @Test
    void createsPhysicalTopicOverHttp() throws Exception {
        // given
        var topicAdmin = new FakeTopicAdmin();
        server = new AdminHttpServer(new AdminConfig(true, "127.0.0.1", 0, null),
                new VirtualTopicManager(Map.of()), new MetadataCache(),
                new FakeGatewayConfigRepository(GatewayConfig.empty()),
                new GovernancePolicy(new GovernanceConfig(null, null)), topicAdmin);
        server.start();

        // when
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + "/topics"))
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"type\":\"physical\",\"name\":\"orders\",\"partitions\":3,\"replicationFactor\":3}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then
        assertThat(response.statusCode()).isEqualTo(201);
        assertThat(topicAdmin.created).hasSize(1);
    }

    @Test
    void createsVirtualTopicOverHttp() throws Exception {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty());
        server = new AdminHttpServer(new AdminConfig(true, "127.0.0.1", 0, null),
                new VirtualTopicManager(Map.of()), new MetadataCache(), repository,
                new GovernancePolicy(new GovernanceConfig(null, null)), new FakeTopicAdmin());
        server.start();

        // when
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + "/topics"))
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"type\":\"virtual\",\"name\":\"orders\",\"topic\":\"orders-v2\"}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then
        assertThat(response.statusCode()).isEqualTo(201);
        assertThat(repository.getActiveConfig().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v2"));
    }

    @Test
    void updatesVirtualTopicOverHttp() throws Exception {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty());
        server = new AdminHttpServer(new AdminConfig(true, "127.0.0.1", 0, null),
                new VirtualTopicManager(Map.of()), new MetadataCache(), repository,
                new GovernancePolicy(new GovernanceConfig(null, null)), new FakeTopicAdmin());
        server.start();

        // when
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + "/topics/orders"))
                        .PUT(HttpRequest.BodyPublishers.ofString("{\"type\":\"virtual\",\"topic\":\"orders-v2\"}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.getActiveConfig().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v2"));
    }

    @Test
    void deletesPhysicalTopicOverHttp() throws Exception {
        // given
        var topicAdmin = new FakeTopicAdmin();
        MetadataCache cache = new MetadataCache();
        cache.update(MetadataSnapshot.of(
                Map.of("orders", TopicMetadata.of("orders",
                        List.of(PartitionMetadata.of(0, 1, List.of(1), List.of(1), List.of())))),
                Map.of(1, BrokerNode.of(1, "localhost", 9092, null)),
                "test-cluster"));
        server = new AdminHttpServer(new AdminConfig(true, "127.0.0.1", 0, null),
                new VirtualTopicManager(Map.of()), cache,
                new FakeGatewayConfigRepository(GatewayConfig.empty()),
                new GovernancePolicy(new GovernanceConfig(null, null)), topicAdmin);
        server.start();

        // when
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + "/topics/orders"))
                        .DELETE()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then
        assertThat(response.statusCode()).isEqualTo(204);
        assertThat(topicAdmin.deleted).containsExactly("orders");
    }

    @Test
    void deletesVirtualTopicOverHttp() throws Exception {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .putVirtualTopic("orders", new VirtualTopicConfig("orders-v2")));
        server = new AdminHttpServer(new AdminConfig(true, "127.0.0.1", 0, null),
                new VirtualTopicManager(Map.of()), new MetadataCache(), repository,
                new GovernancePolicy(new GovernanceConfig(null, null)), new FakeTopicAdmin());
        server.start();

        // when
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + "/topics/orders"))
                        .DELETE()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then
        assertThat(response.statusCode()).isEqualTo(204);
        assertThat(repository.getActiveConfig().virtualTopics()).isEmpty();
    }
}
