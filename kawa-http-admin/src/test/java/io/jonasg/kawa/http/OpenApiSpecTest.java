package io.jonasg.kawa.http;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.dataformat.yaml.YAMLMapper;

import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;

/// Guards the shipped OpenAPI spec: it must parse as YAML and document the admin surface's
/// routes. Keeps a syntax error or a dropped route from shipping silently.
class OpenApiSpecTest {

    private final YAMLMapper yaml = YAMLMapper.builder().build();

    @Test
    void specParsesAndDocumentsTheAdminSurface() throws Exception {
        // given - the OpenAPI spec shipped with the module
        byte[] spec;
        try (InputStream in = getClass().getResourceAsStream("/openapi.yaml")) {
            assertThat(in).as("openapi.yaml must be on the classpath").isNotNull();
            spec = in.readAllBytes();
        }

        // when - parsed as YAML
        JsonNode root = yaml.readTree(spec);

        // then - it is an OpenAPI 3.0 document describing every admin route
        assertThat(root.path("openapi").asText()).isEqualTo("3.0.3");
        assertThat(root.path("paths").path("/topics").has("get")).isTrue();
        assertThat(root.path("paths").path("/topics").has("post")).isTrue();
        assertThat(root.path("paths").path("/topics/{name}").has("put")).isTrue();
        assertThat(root.path("paths").path("/topics/{name}").has("delete")).isTrue();
        assertThat(root.path("paths").path("/rbac/roles").has("get")).isTrue();
        assertThat(root.path("paths").path("/rbac/roles/{name}").has("put")).isTrue();
        assertThat(root.path("paths").path("/rbac/roles/{name}").has("delete")).isTrue();
        assertThat(root.path("paths").path("/rbac/groups").has("get")).isTrue();
        assertThat(root.path("paths").path("/rbac/groups/{name}").has("put")).isTrue();
        assertThat(root.path("paths").path("/rbac/groups/{name}").has("delete")).isTrue();
        assertThat(root.path("paths").path("/auth/users").has("get")).isTrue();
        assertThat(root.path("paths").path("/auth/users/{name}").has("put")).isTrue();
        assertThat(root.path("paths").path("/auth/users/{name}").has("delete")).isTrue();
        assertThat(root.path("paths").path("/governance").has("get")).isTrue();
        assertThat(root.path("paths").path("/governance").has("put")).isTrue();
    }
}
