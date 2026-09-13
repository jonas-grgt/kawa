package io.jonasg.kawa.it;

import java.net.URI;
import java.net.http.HttpRequest;

public class AdminHTTPTestSupport extends GatewayTestSupport {

	HttpRequest.Builder reqBuilderForPath(String path) {
		return HttpRequest.newBuilder(
				URI.create("http://127.0.0.1:" + gateway.adminBoundPort() + path));
	}

}
