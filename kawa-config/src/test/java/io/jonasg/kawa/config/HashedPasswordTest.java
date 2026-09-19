package io.jonasg.kawa.config;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class HashedPasswordTest {

    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    void equalPlaintextAndSaltProduceEqualPasswords() {
        // given
        var password = new HashedPassword("secret", "salt");
        var otherPassword = new HashedPassword("secret", "salt");

        // when / then
        assertThat(password).isEqualTo(otherPassword);
        assertThat(password).hasSameHashCodeAs(otherPassword);
    }

    @Test
    void differentPlaintextOrSaltProducesDifferentPasswords() {
        // given
        var password = new HashedPassword("secret", "salt");

        // when / then
        assertThat(password).isNotEqualTo(new HashedPassword("other", "salt"));
        assertThat(password).isNotEqualTo(new HashedPassword("secret", "other-salt"));
    }

    @Test
    void clientPasswordSerializesAsAStringAndDeserializesBackToAValue() throws Exception {
        // given
        var client = new ClientConfig("PLAIN", new HashedPassword("secret", "salt"));

        // when
        String json = mapper.writeValueAsString(client);
        ClientConfig readBack = mapper.readValue(json, ClientConfig.class);

        // then
        assertThat(json).contains("\"password\":\"pbkdf2-sha256$");
        assertThat(readBack).isEqualTo(client);
    }
}
