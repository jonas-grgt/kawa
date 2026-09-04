package io.jonasg.kawa.http;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RouterTest {

    private static final String JSON = "application/json";

    @Test
    void routesGetToRegisteredHandler() {
        // given
        var router = new Router().get("/topics", List::of);
        var handler = new HttpRouterHandler(router);

        // when
        FullHttpResponse response = request(handler, HttpMethod.GET, "/topics");

        // then
        assertThat(response.status()).isEqualTo(HttpResponseStatus.OK);
        assertThat(response.headers().get("Content-Type")).isEqualTo(JSON);
        assertThat(response.content().toString(StandardCharsets.UTF_8)).isEqualTo("[]");
    }

    @Test
    void returnsNotFoundForUnknownPath() {
        // given
        var handler = new HttpRouterHandler(new Router().get("/topics", List::of));

        // when
        FullHttpResponse response = request(handler, HttpMethod.GET, "/unknown");

        // then
        assertThat(response.status()).isEqualTo(HttpResponseStatus.NOT_FOUND);
    }

    @Test
    void returnsMethodNotAllowedForKnownPathWithWrongMethod() {
        // given
        var handler = new HttpRouterHandler(new Router().get("/topics", List::of));

        // when
        FullHttpResponse response = request(handler, HttpMethod.POST, "/topics");

        // then
        assertThat(response.status()).isEqualTo(HttpResponseStatus.METHOD_NOT_ALLOWED);
    }

    @Test
    void setsKeepAliveHeaderWhenRequestIsKeepAlive() {
        // given
        var handler = new HttpRouterHandler(new Router().get("/topics", List::of));

        // when
        FullHttpResponse response = request(handler, HttpMethod.GET, "/topics");

        // then
        assertThat(response.headers().get("Connection")).isEqualTo("keep-alive");
    }

    private static FullHttpResponse request(HttpRouterHandler handler, HttpMethod method, String path) {
        var channel = new EmbeddedChannel(handler);
        FullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, method, path, Unpooled.EMPTY_BUFFER);
        channel.writeInbound(request);
        FullHttpResponse response = channel.readOutbound();
        channel.finish();
        return response;
    }
}
