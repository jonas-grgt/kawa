package io.jonasg.kawa.rbac;

import io.jonasg.kawa.core.GatewayContext;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AuthorizationCheckRegistryTest {

    private static GatewayContext context() {
        return new GatewayContext("source", 0L);
    }

    @Test
    void dispatchesOnRequestToTheCheckForTheApiKey() {
        var first = new RecordingCheck((short) 1);
        var second = new RecordingCheck((short) 2);
        var registry = new AuthorizationCheckRegistry(List.of(first, second));

        registry.onRequest(context(), (short) 1, (short) 9, "body-1");
        registry.onRequest(context(), (short) 2, (short) 5, "body-2");

        assertThat(first.requestBodies).containsExactly("body-1");
        assertThat(first.requestVersions).containsExactly((short) 9);
        assertThat(second.requestBodies).containsExactly("body-2");
        assertThat(second.requestVersions).containsExactly((short) 5);
    }

    @Test
    void dispatchesOnResponseToTheCheckForTheApiKey() {
        var first = new RecordingCheck((short) 1);
        var second = new RecordingCheck((short) 2);
        var registry = new AuthorizationCheckRegistry(List.of(first, second));

        registry.onResponse(context(), (short) 1, "resp-1");
        registry.onResponse(context(), (short) 2, "resp-2");

        assertThat(first.responseBodies).containsExactly("resp-1");
        assertThat(second.responseBodies).containsExactly("resp-2");
    }

    @Test
    void doesNotDispatchForAnUnregisteredApiKey() {
        var check = new RecordingCheck((short) 1);
        var registry = new AuthorizationCheckRegistry(List.of(check));

        registry.onRequest(context(), (short) 99, (short) 0, "body");
        registry.onResponse(context(), (short) 99, "resp");

        assertThat(check.requestBodies).isEmpty();
        assertThat(check.responseBodies).isEmpty();
    }

    @Test
    void doesNotDispatchOnResponseForANullBody() {
        var check = new RecordingCheck((short) 1);
        var registry = new AuthorizationCheckRegistry(List.of(check));

        registry.onResponse(context(), (short) 1, null);

        assertThat(check.responseBodies).isEmpty();
    }

    @Test
    void hasApiKeyReflectsRegisteredKeys() {
        var registry = new AuthorizationCheckRegistry(List.of(new RecordingCheck((short) 1)));

        assertThat(registry.hasApiKey((short) 1)).isTrue();
        assertThat(registry.hasApiKey((short) 2)).isFalse();
    }

    @Test
    void rejectsDuplicateApiKeys() {
        assertThatThrownBy(() -> new AuthorizationCheckRegistry(List.of(
                new RecordingCheck((short) 1),
                new RecordingCheck((short) 1))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate authorization check for api key 1");
    }

    private static final class RecordingCheck implements AuthorizationCheck<Object, Object> {

        private final short apiKey;
        private final java.util.ArrayList<Object> requestBodies = new java.util.ArrayList<>();
        private final java.util.ArrayList<Short> requestVersions = new java.util.ArrayList<>();
        private final java.util.ArrayList<Object> responseBodies = new java.util.ArrayList<>();

        private RecordingCheck(short apiKey) {
            this.apiKey = apiKey;
        }

        @Override
        public short apiKey() {
            return apiKey;
        }

        @Override
        public void onRequest(GatewayContext context, short apiVersion, Object body) {
            requestBodies.add(body);
            requestVersions.add(apiVersion);
        }

        @Override
        public void onResponse(GatewayContext context, Object body) {
            responseBodies.add(body);
        }
    }
}
