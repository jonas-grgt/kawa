package io.jonasg.kawa.config;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfigLoaderTest {

    private final ConfigLoader loader = new ConfigLoader();

    @Test
    void loadsFullConfiguration() {
        GatewayConfig config = loader.loadFromYaml("""
                name: test-gateway
                listeners:
                  - host: 0.0.0.0
                    port: 9092
                clusters:
                  default:
                    bootstrapServers:
                      - localhost:9093
                      - localhost:9094
                virtualTopics:
                  orders: orders-v2
                  customers:
                    topic: crm.customers
                advertised:
                  nodeId: 7
                  host: gw.example.com
                  port: 9092
                metrics:
                  enabled: true
                  prometheusPort: 9095
                """);

        assertThat(config.name()).isEqualTo("test-gateway");
        assertThat(config.listeners()).containsExactly(new ListenerConfig("0.0.0.0", 9092));
        ClusterConfig cluster = config.defaultCluster();
        assertThat(cluster.bootstrapServers()).containsExactly("localhost:9093", "localhost:9094");
        assertThat(config.virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v2"))
                .containsEntry("customers", new VirtualTopicConfig("crm.customers"));
        assertThat(config.advertised()).isEqualTo(new AdvertisedListener(7, "gw.example.com", 9092));
        assertThat(config.metrics().enabled()).isTrue();
        assertThat(config.metrics().prometheusPort()).isEqualTo(9095);
    }

    @Test
    void appliesDefaults() {
        GatewayConfig config = loader.loadFromYaml("listeners:\n  - port: 9092\n");

        assertThat(config.name()).isEqualTo("kafka-gateway");
        assertThat(config.listeners()).containsExactly(new ListenerConfig("0.0.0.0", 9092));
        assertThat(config.clusters()).isEmpty();
        assertThat(config.virtualTopics()).isEmpty();
        assertThat(config.advertised()).isEqualTo(new AdvertisedListener(1, "localhost", 9092));
        assertThat(config.metrics().enabled()).isFalse();
        assertThat(config.auth().mechanisms()).isEmpty();
        assertThat(config.auth().users()).isEmpty();
        assertThat(config.auth().brokerAuth()).isNull();
        assertThat(config.admin().enabled()).isFalse();
    }

    @Test
    void ignoresUnknownKeys() {
        GatewayConfig config = loader.loadFromYaml("""
                someFutureKey: 42
                listeners:
                  - port: 9092
                """);

        assertThat(config.listeners()).hasSize(1);
    }

    @Test
    void configurationIsImmutable() {
        GatewayConfig config = loader.loadFromYaml("virtualTopics:\n  orders: orders-v2\n");

        assertThatThrownBy(() -> config.virtualTopics().put("a", new VirtualTopicConfig("b")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsInvalidVirtualTopicMapping() {
        assertThatThrownBy(() -> loader.loadFromYaml("virtualTopics:\n  orders:\n    - not-a-topic-map\n"))
                .hasMessageContaining("Invalid virtual topic mapping for topic 'orders'");
    }

    @Test
    void loadsVirtualTopicObjectWithUnknownKeys() {
        GatewayConfig config = loader.loadFromYaml("""
                virtualTopics:
                  customers:
                    topic: crm.customers
                    unknown: ignored
                """);

        assertThat(config.virtualTopics())
                .containsEntry("customers", new VirtualTopicConfig("crm.customers"));
    }

    @Test
    void loadsVirtualTopicFilterConfiguration() {
        GatewayConfig config = loader.loadFromYaml("""
                virtualTopics:
                  customers:
                    topic: crm.customers
                    filter:
                      type: headerEquals
                      header: tenant
                      value: acme
                """);

        assertThat(config.virtualTopics().get("customers").filter())
                .isEqualTo(new HeaderEqualsFilterConfig("tenant", "acme"));
    }

    @Test
    void loadsCelFilterConfiguration() {
        GatewayConfig config = loader.loadFromYaml("""
                virtualTopics:
                  orders:
                    topic: orders-v2
                    filter:
                      type: cel
                      expression: headers.tenant == "acme"
                """);

        assertThat(config.virtualTopics().get("orders").filter())
                .isEqualTo(new CelFilterConfig("headers.tenant == \"acme\""));
    }

    @Test
    void virtualTopicPhysicalNameIsHiddenByDefault() {
        GatewayConfig config = loader.loadFromYaml("""
                virtualTopics:
                  orders: orders-v2
                  customers:
                    topic: crm.customers
                """);

        assertThat(config.virtualTopics().get("orders").exposePhysicalTopic()).isFalse();
        assertThat(config.virtualTopics().get("customers").exposePhysicalTopic()).isFalse();
    }

    @Test
    void loadsExposePhysicalTopicOptIn() {
        GatewayConfig config = loader.loadFromYaml("""
                virtualTopics:
                  legacy:
                    topic: legacy-v1
                    exposePhysicalTopic: true
                """);

        assertThat(config.virtualTopics().get("legacy").exposePhysicalTopic()).isTrue();
    }

    @Test
    void rejectsNonBooleanExposePhysicalTopic() {
        assertThatThrownBy(() -> loader.loadFromYaml("""
                virtualTopics:
                  customers:
                    topic: crm.customers
                    exposePhysicalTopic: "yes"
                """))
                .hasMessageContaining("exposePhysicalTopic");
    }

    @Test
    void rejectsVirtualTopicFilterWithoutType() {
        assertThatThrownBy(() -> loader.loadFromYaml("""
                virtualTopics:
                  customers:
                    topic: crm.customers
                    filter:
                      header: tenant
                      value: acme
                """))
                .hasMessageContaining("Invalid filter config for virtual topic 'customers'")
                .hasMessageContaining("type");
    }

    @Test
    void rejectsUnknownVirtualTopicFilterType() {
        assertThatThrownBy(() -> loader.loadFromYaml("""
                virtualTopics:
                  customers:
                    topic: crm.customers
                    filter:
                      type: notARealType
                      header: tenant
                      value: acme
                """))
                .hasMessageContaining("Invalid filter config for virtual topic 'customers'")
                .hasMessageContaining("type");
    }

    @Test
    void loadsFullAuthConfiguration() {
        GatewayConfig config = loader.loadFromYaml("""
                auth:
                  mechanisms:
                    - PLAIN
                    - SCRAM-SHA-256
                  users:
                    alice:
                      password: s3cret
                    bob:
                      mechanism: SCRAM-SHA-256
                      password: hunter2
                listeners:
                  - port: 9092
                """);

        assertThat(config.auth().mechanisms()).containsExactlyInAnyOrder("PLAIN", "SCRAM-SHA-256");
        assertThat(config.auth().users()).hasSize(2);
        assertThat(config.auth().users().get("alice").mechanism()).isEqualTo("PLAIN");
        assertThat(config.auth().users().get("alice").password()).isEqualTo("s3cret");
        assertThat(config.auth().users().get("bob").mechanism()).isEqualTo("SCRAM-SHA-256");
        assertThat(config.auth().users().get("bob").password()).isEqualTo("hunter2");
    }

    @Test
    void rejectsUserWithoutMechanismWhenNoGlobalMechanism() {
        assertThatThrownBy(() -> loader.loadFromYaml("""
                auth:
                  users:
                    alice:
                      password: s3cret
                listeners:
                  - port: 9092
                """))
                .hasMessageContaining("alice")
                .hasMessageContaining("mechanism");
    }

    @Test
    void authConfigurationIsImmutable() {
        GatewayConfig config = loader.loadFromYaml("""
                auth:
                  mechanisms:
                    - PLAIN
                  users:
                    alice:
                      password: s3cret
                listeners:
                  - port: 9092
                """);

        assertThatThrownBy(() -> config.auth().mechanisms().add("SCRAM-SHA-512"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> config.auth().users().put("bob", new UserConfig("PLAIN", "pw")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void loadsBrokerAuthConfiguration() {
        GatewayConfig config = loader.loadFromYaml("""
                auth:
                  brokerAuth:
                    mechanism: PLAIN
                    username: gateway-service
                    password: s3cret
                listeners:
                  - port: 9092
                """);

        assertThat(config.auth().brokerAuth()).isNotNull();
        assertThat(config.auth().brokerAuth().mechanism()).isEqualTo("PLAIN");
        assertThat(config.auth().brokerAuth().username()).isEqualTo("gateway-service");
        assertThat(config.auth().brokerAuth().password()).isEqualTo("s3cret");
    }

    @Test
    void brokerAuthDefaultIsNull() {
        GatewayConfig config = loader.loadFromYaml("""
                auth:
                  mechanisms:
                    - PLAIN
                  users:
                    alice:
                      password: secret
                listeners:
                  - port: 9092
                """);

        assertThat(config.auth().brokerAuth()).isNull();
    }

    @Test
    void loadsRbacConfiguration() {
        GatewayConfig config = loader.loadFromYaml("""
                rbac:
                  roles:
                    producer:
                      acls:
                        - resource:
                            type: TOPIC
                            pattern: orders
                            patternType: LITERAL
                          operation: WRITE
                          permission: ALLOW
                    admin:
                      acls:
                        - resource:
                            type: CLUSTER
                          operation: CREATE
                    consumer:
                      acls:
                        - resource:
                            type: GROUP
                            pattern: orders-group
                          operation: READ
                  groups:
                    producers:
                      members: [alice]
                      roles: [producer]
                    admins:
                      members: [bob]
                      roles: [admin]
                    consumers:
                      members: [carol]
                      roles: [consumer]
                """);

        RbacConfig rbac = config.rbac();
        assertThat(rbac.roles().keySet()).containsExactlyInAnyOrder("producer", "admin", "consumer");
        assertThat(rbac.roles().get("producer").acls()).containsExactly(
                new AclConfig(new ResourceConfig(ResourceType.TOPIC, "orders", PatternType.LITERAL),
                        AclOperation.WRITE, AclPermissionType.ALLOW));
        assertThat(rbac.roles().get("admin").acls()).containsExactly(
                new AclConfig(new ResourceConfig(ResourceType.CLUSTER, null, PatternType.LITERAL),
                        AclOperation.CREATE, AclPermissionType.ALLOW));
        assertThat(rbac.roles().get("consumer").acls()).containsExactly(
                new AclConfig(new ResourceConfig(ResourceType.GROUP, "orders-group", PatternType.LITERAL),
                        AclOperation.READ, AclPermissionType.ALLOW));
        assertThat(rbac.groups().get("producers").members()).containsExactly("alice");
        assertThat(rbac.groups().get("producers").roles()).containsExactly("producer");
        assertThat(rbac.groups().get("admins").members()).containsExactly("bob");
        assertThat(rbac.groups().get("consumers").members()).containsExactly("carol");
    }

    @Test
    void loadsAdminConfiguration() {
        GatewayConfig config = loader.loadFromYaml("""
                admin:
                  enabled: true
                  host: 127.0.0.1
                  port: 8081
                """);

        assertThat(config.admin().enabled()).isTrue();
        assertThat(config.admin().host()).isEqualTo("127.0.0.1");
        assertThat(config.admin().port()).isEqualTo(8081);
        assertThat(config.admin().cors()).isNull();
    }

    @Test
    void loadsAdminCorsConfiguration() {
        GatewayConfig config = loader.loadFromYaml("""
                admin:
                  enabled: true
                  host: 127.0.0.1
                  port: 8081
                  cors:
                    allowedOrigins:
                      - http://localhost:8080
                      - http://localhost:5173
                    allowedMethods:
                      - GET
                      - OPTIONS
                    allowedHeaders:
                      - Content-Type
                    allowCredentials: true
                    maxAge: 3600
                """);

        CorsConfig cors = config.admin().cors();
        assertThat(cors).isNotNull();
        assertThat(cors.allowedOrigins())
                .containsExactly("http://localhost:8080", "http://localhost:5173");
        assertThat(cors.allowedMethods()).containsExactly("GET", "OPTIONS");
        assertThat(cors.allowedHeaders()).containsExactly("Content-Type");
        assertThat(cors.allowCredentials()).isTrue();
        assertThat(cors.maxAge()).isEqualTo(3600L);
    }

    @Test
    void adminCorsDefaultsToDisabled() {
        GatewayConfig config = loader.loadFromYaml("listeners:\n  - port: 9092\n");

        assertThat(config.admin().enabled()).isFalse();
        assertThat(config.admin().host()).isEqualTo("0.0.0.0");
        assertThat(config.admin().port()).isEqualTo(8080);
        assertThat(config.admin().cors()).isNull();
    }
}
