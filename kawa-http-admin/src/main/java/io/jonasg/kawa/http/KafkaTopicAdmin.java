package io.jonasg.kawa.http;

import io.jonasg.kawa.config.BrokerAuthConfig;
import io.jonasg.kawa.governance.TopicSpec;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.List;
import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;

/// [TopicAdmin] backed by Kafka's [AdminClient]: creates and deletes topics on the real
/// cluster using the same bootstrap servers and broker credentials as the gateway.
public final class KafkaTopicAdmin implements TopicAdmin {

    private final AdminClient admin;

    public KafkaTopicAdmin(String bootstrapServers, BrokerAuthConfig brokerAuth) {
        this(AdminClient.create(props(bootstrapServers, brokerAuth)));
    }

    /// Package-private for tests.
    KafkaTopicAdmin(AdminClient admin) {
        this.admin = admin;
    }

    @Override
    public void createTopic(TopicSpec spec) throws Exception {
        var topic = new NewTopic(spec.name(), spec.partitions(), (short) spec.replicationFactor())
                .configs(spec.configs());
        admin.createTopics(List.of(topic)).all().get();
    }

    @Override
    public void deleteTopic(String name) throws Exception {
        admin.deleteTopics(List.of(name)).all().get();
    }

    @Override
    public void close() {
        admin.close();
    }

    /// AdminClient properties: bootstrap servers plus the gateway's broker SASL credentials
    /// when configured.
    static Properties props(String bootstrapServers, BrokerAuthConfig brokerAuth) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        if (brokerAuth != null) {
            props.put(SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            props.put(SaslConfigs.SASL_MECHANISM, brokerAuth.mechanism());
            props.put(SaslConfigs.SASL_JAAS_CONFIG,
                    "org.apache.kafka.common.security.plain.PlainLoginModule required "
                            + "username=\"" + brokerAuth.username() + "\" password=\"" + brokerAuth.password() + "\";");
        }
        return props;
    }
}