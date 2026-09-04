package io.jonasg.kawa.http;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.List;

/// The single Netty dispatcher in the admin HTTP pipeline. It decodes a [FullHttpRequest], routes it
/// through the [Router] to a plain handler, and writes the JSON response (handling keep-alive,
/// content-type, 404 and 405). All HTTP plumbing lives here so route handlers stay Netty-free.
public final class HttpRouterHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final String JSON = "application/json";

    private final Router router;
    private final JsonMapper mapper;

    public HttpRouterHandler(Router router) {
        this.router = router;
        this.mapper = JsonMapper.builder().build();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        String path = request.uri();
        if (!router.hasPath(path)) {
            write(ctx, request, HttpResponseStatus.NOT_FOUND, "{\"error\":\"not found\"}");
            return;
        }
        Router.Handler<?> handler = router.find(request.method(), path).orElse(null);
        if (handler == null) {
            write(ctx, request, HttpResponseStatus.METHOD_NOT_ALLOWED, "{\"error\":\"method not allowed\"}");
            return;
        }
        Object topics = handler.handle();
        byte[] body;
        try {
            body = mapper.writeValueAsBytes(topics);
        } catch (Exception e) {
            write(ctx, request, HttpResponseStatus.INTERNAL_SERVER_ERROR, "{\"error\":\"serialization failed\"}");
            return;
        }
        write(ctx, request, HttpResponseStatus.OK, body);
    }

    private static void write(
            ChannelHandlerContext ctx,
            FullHttpRequest request,
            HttpResponseStatus status,
            String body
    ) {
        write(ctx, request, status, body.getBytes(StandardCharsets.UTF_8));
    }

    private static void write(
            ChannelHandlerContext ctx,
            FullHttpRequest request,
            HttpResponseStatus status,
            byte[] body
    ) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, status, Unpooled.wrappedBuffer(body));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, JSON);
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, body.length);
        boolean keepAlive = HttpUtil.isKeepAlive(request);
        if (keepAlive) {
            response.headers().set(HttpHeaderNames.CONNECTION, "keep-alive");
        }
        ctx.writeAndFlush(response).addListener(future -> {
            if (!keepAlive) {
                ctx.close();
            }
        });
    }
}
