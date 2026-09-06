package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CorsConfigTest {

    @Test
    void appliesDefaults() {
        // given / when
        CorsConfig config = new CorsConfig(null, null, null, null, null);

        // then
        assertThat(config.allowedOrigins()).containsExactly("*");
        assertThat(config.allowedMethods()).containsExactly("GET");
        assertThat(config.allowedHeaders()).containsExactly("*");
        assertThat(config.allowCredentials()).isFalse();
        assertThat(config.maxAge()).isNull();
    }

    @Test
    void keepsExplicitValues() {
        // given / when
        CorsConfig config = CorsConfig.of(
                List.of("http://localhost:8080", "http://localhost:5173"),
                List.of("GET", "OPTIONS"),
                List.of("Content-Type"),
                true,
                3600L);

        // then
        assertThat(config.allowedOrigins())
                .containsExactly("http://localhost:8080", "http://localhost:5173");
        assertThat(config.allowedMethods()).containsExactly("GET", "OPTIONS");
        assertThat(config.allowedHeaders()).containsExactly("Content-Type");
        assertThat(config.allowCredentials()).isTrue();
        assertThat(config.maxAge()).isEqualTo(3600L);
    }

    @Test
    void copiesCollectionsSoConfigStaysImmutable() {
        // given
        List<String> origins = new java.util.ArrayList<>(List.of("http://localhost:8080"));

        // when
        CorsConfig config = new CorsConfig(origins, null, null, null, null);
        origins.add("http://evil.example.com");

        // then
        assertThat(config.allowedOrigins()).containsExactly("http://localhost:8080");
    }
}
