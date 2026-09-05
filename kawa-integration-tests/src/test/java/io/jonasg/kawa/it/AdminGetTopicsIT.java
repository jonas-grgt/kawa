package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AdminConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AdminGetTopicsIT extends GatewayTestSupport {

	@Override
	protected AdminConfig adminConfig() {
		return new AdminConfig(true, "127.0.0.1", 0);
	}

	@Override
	protected Map<String, String> virtualTopics() {
		return Map.of("orders", "orders-v2");
	}

	@Override
	protected List<NewTopic> initialTopics() {
		return List.of(new NewTopic("orders-v2", 1, (short) 1));
	}

	@Test
	void exposesVirtualTopicsOverHttp() {
		// given
		var request = HttpRequest.newBuilder(
						URI.create("http://127.0.0.1:" + gateway.adminBoundPort() + "/topics"))
				.GET()
				.build();

		// when — the gateway's metadata cache picks up the pre-created topic on its next refresh
		Awaitility.await()
				.atMost(Duration.ofSeconds(30))
				.pollInterval(Duration.ofMillis(500))
				.untilAsserted(() -> {
					HttpResponse<String> response = HttpClient.newHttpClient()
							.send(request, HttpResponse.BodyHandlers.ofString());

					// then
					assertThat(response.statusCode()).isEqualTo(200);
					JSONAssert.assertEquals("""
							[{"logicalName":"orders","physicalName":"orders-v2","partitionCount":1,"filter":null,"exposePhysicalTopic":false}]
							""", response.body(), JSONCompareMode.LENIENT);
				});
	}
}