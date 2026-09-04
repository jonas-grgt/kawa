package io.jonasg.kawa.it;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.InternetProtocol;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.config.AclConfig;
import io.jonasg.kawa.config.AuthConfig;
import io.jonasg.kawa.config.BrokerAuthConfig;
import io.jonasg.kawa.config.ClusterConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.ListenerConfig;
import io.jonasg.kawa.config.MetricsConfig;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.config.ResourceConfig;
import io.jonasg.kawa.config.RoleConfig;
import io.jonasg.kawa.config.UserConfig;
import io.jonasg.kawa.server.KafkaGateway;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/// Wire-level check that the gateway authenticates to a SASL-enabled upstream broker
/// using the configured broker credentials, instead of connecting in plaintext.
///
/// Uses [GenericContainer] with `confluentinc/cp-kafka` rather than
/// `KafkaContainer` so we have full control over listener configuration and SASL setup.
/// The container's command is overridden to inject a JAAS config file before starting Kafka.
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BrokerAuthIT {

    static final String BROKER_USER = "gateway";
    static final String BROKER_PASSWORD = "gateway-secret";
    static final int SASL_PORT = 19093;

    /// Port 19093 is bound to a fixed host port so that the broker's advertised
    /// listener (`localhost:19093`) is reachable from the host where the
    /// gateway runs.
    static GenericContainer<?> kafka = new GenericContainer<>("confluentinc/cp-kafka:7.6.0")
            .withEnv("KAFKA_NODE_ID", "1")
            .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
            .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:29093")
            .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
            .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                    "CONTROLLER:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT")
            .withEnv("KAFKA_LISTENERS",
                    "CONTROLLER://0.0.0.0:29093,SASL_PLAINTEXT://0.0.0.0:" + SASL_PORT)
            .withEnv("KAFKA_ADVERTISED_LISTENERS",
                    "SASL_PLAINTEXT://localhost:" + SASL_PORT)
            .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
            .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
            .withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN")
            .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "SASL_PLAINTEXT")
            .withEnv("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostConfig(
                    HostConfig.newHostConfig()
                            .withPortBindings(new PortBinding(
                                    Ports.Binding.bindPort(SASL_PORT),
                                    ExposedPort.tcp(SASL_PORT)))))
            .withCommand("bash", "-c",
                    "mkdir -p /etc/kafka/secrets && "
                            + "cat > /etc/kafka/secrets/kafka_server_jaas.conf << 'EOF'\n"
                            + "KafkaServer {\n"
                            + "  org.apache.kafka.common.security.plain.PlainLoginModule required\n"
                            + "  username=\"" + BROKER_USER + "\"\n"
                            + "  password=\"" + BROKER_PASSWORD + "\"\n"
                            + "  user_" + BROKER_USER + "=\"" + BROKER_PASSWORD + "\";\n"
                            + "};\n"
                            + "EOF\n"
                            + "export KAFKA_OPTS='-Djava.security.auth.login.config=/etc/kafka/secrets/kafka_server_jaas.conf' && "
                            + "/etc/confluent/docker/run")
            .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1));

    private static KafkaGateway gateway;
    private static String gatewayBootstrap;

    @BeforeAll
    void setUp() throws Exception {
        kafka.start();
        String brokerBootstrap = "localhost:" + SASL_PORT;

        var auth = new AuthConfig(
                Set.of("PLAIN"),
                Map.of("client", new UserConfig("PLAIN", "client-secret")),
                new BrokerAuthConfig("PLAIN", BROKER_USER, BROKER_PASSWORD));

        var allowAllRole = new RoleConfig(List.of(
                new AclConfig(new ResourceConfig(ResourceType.TOPIC, "", PatternType.PREFIXED), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.GROUP, "", PatternType.PREFIXED), AclOperation.ALL),
                new AclConfig(new ResourceConfig(ResourceType.CLUSTER, null), AclOperation.ALL)));
        var rbac = new RbacConfig(
                Map.of("allow-all", allowAllRole),
                Map.of("clients", new GroupConfig(List.of("client"), List.of("allow-all"))));

        var config = new GatewayConfig(
                "test-gateway",
                List.of(new ListenerConfig("127.0.0.1", 0)),
                Map.of("default", new ClusterConfig("default", List.of(brokerBootstrap))),
                Map.of(),
                new AdvertisedListener(1, "localhost", 0),
                new MetricsConfig(false, 0),
                auth,
                rbac,
                null);

        gateway = new KafkaGateway(config);
        gateway.start();
        gatewayBootstrap = "localhost:" + gateway.boundPort();
    }

    @AfterAll
    void tearDown() {
        if (gateway != null) {
            gateway.stop();
        }
        if (kafka != null) {
            kafka.stop();
        }
    }

    @Test
    void gatewayProducesThroughSaslAuthenticatedBrokerConnection() throws Exception {
        var producerProps = new Properties();
        producerProps.put("bootstrap.servers", gatewayBootstrap);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        producerProps.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        producerProps.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"client\" password=\"client-secret\";");

        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(producerProps)) {
            producer.send(new ProducerRecord<>("broker-auth-test", "key", "value")).get();
        }

        var consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", gatewayBootstrap);
        consumerProps.put("group.id", "broker-auth-it");
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        consumerProps.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        consumerProps.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"client\" password=\"client-secret\";");

        try (var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(consumerProps)) {
            consumer.subscribe(List.of("broker-auth-test"));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records).hasSize(1);
            assertThat(records.iterator().next().value()).isEqualTo("value");
        }
    }
}
