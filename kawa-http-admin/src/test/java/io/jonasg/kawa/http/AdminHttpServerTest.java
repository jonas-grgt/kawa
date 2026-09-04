package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.core.VirtualTopicManager;
import io.jonasg.kawa.core.cluster.BrokerNode;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.cluster.MetadataSnapshot;
import io.jonasg.kawa.core.cluster.PartitionMetadata;
import io.jonasg.kawa.core.cluster.TopicMetadata;
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
        server = new AdminHttpServer(new AdminConfig(true, "127.0.0.1", 0), virtualTopics, cache);
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
                "\"logicalName\":\"orders\"",
                "\"physicalName\":\"orders-v2\"",
                "\"partitionCount\":1");
    }
}
