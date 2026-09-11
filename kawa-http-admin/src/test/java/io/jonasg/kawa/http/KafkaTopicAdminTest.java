package io.jonasg.kawa.http;

import io.jonasg.kawa.config.BrokerAuthConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaTopicAdminTest {

    @Test
    void buildsPlaintextPropertiesWithoutBrokerAuth() {
        // given
        // when
        var props = KafkaTopicAdmin.props("localhost:9092", null);

        // then
        assertThat(props.getProperty("bootstrap.servers")).isEqualTo("localhost:9092");
        assertThat(props.getProperty("security.protocol")).isNull();
    }

    @Test
    void buildsSaslPropertiesWithBrokerAuth() {
        // given
        var auth = new BrokerAuthConfig("PLAIN", "kawa", "secret");

        // when
        var props = KafkaTopicAdmin.props("localhost:9092", auth);

        // then
        assertThat(props.getProperty("bootstrap.servers")).isEqualTo("localhost:9092");
        assertThat(props.getProperty("security.protocol")).isEqualTo("SASL_PLAINTEXT");
        assertThat(props.getProperty("sasl.mechanism")).isEqualTo("PLAIN");
        assertThat(props.getProperty("sasl.jaas.config")).contains("username=\"kawa\"", "password=\"secret\"");
    }
}