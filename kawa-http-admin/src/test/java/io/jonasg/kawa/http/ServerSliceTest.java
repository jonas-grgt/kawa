package io.jonasg.kawa.http;

import io.jonasg.kawa.config.CorsConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/// Slice tests for the [AdminHttpServer] bootstrap surface that has no handler equivalent:
/// the rendered OpenAPI docs and the CORS pipeline.
class ServerSliceTest extends AdminHttpSliceTestBase {

    @Test
    void servesRenderedOpenApiDocsOverHttp() throws Exception {
        // given
        startServer();

        // when
        var response = send("GET", "/docs", null);

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Content-Type")).contains("text/html");
        assertThat(response.body()).contains("Kawa Admin API");
    }

    @Test
    void servesPreflightRequestWithCorsHeadersWhenConfigured() throws Exception {
        // given
        cors = new CorsConfig(List.of("http://localhost:8080"), null, null, null, null);
        startServer();

        // when
        var response = send("OPTIONS", "/topics", null, Map.of(
                "Origin", "http://localhost:8080",
                "Access-Control-Request-Method", "GET"));

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
        cors = new CorsConfig(List.of("http://localhost:8080"), null, null, null, null);
        startServer();

        // when
        var response = send("GET", "/topics", null, Map.of("Origin", "http://localhost:8080"));

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Access-Control-Allow-Origin"))
                .contains("http://localhost:8080");
    }

    @Test
    void doesNotAddCorsHeadersWhenNotConfigured() throws Exception {
        // given
        startServer();

        // when
        var response = send("GET", "/topics", null, Map.of("Origin", "http://localhost:8080"));

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Access-Control-Allow-Origin")).isEmpty();
    }
}