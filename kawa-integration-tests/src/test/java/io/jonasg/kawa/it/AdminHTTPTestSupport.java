package io.jonasg.kawa.it;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class AdminHTTPTestSupport extends GatewayTestSupport {

	protected HttpClient http = HttpClient.newHttpClient();

	String baseUrl() {
		return "http://127.0.0.1:" + gateway.adminBoundPort();
	}

	HttpRequest.Builder reqBuilder(String path) {
		if (!path.startsWith("/")) {
			throw new IllegalArgumentException("Path must start with '/'");
		}
		return HttpRequest.newBuilder(
				URI.create(baseUrl() + path));
	}

	HttpResponse<String> httpExec(HttpRequest request) throws Exception {
		return http.send(request, HttpResponse.BodyHandlers.ofString());
	}

}
