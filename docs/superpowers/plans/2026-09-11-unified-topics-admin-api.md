# Unified `/topics` Admin API + Flattened Config Surface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unify the admin API's topic surface into a single `/topics` resource (GET/POST/PUT/DELETE covering both physical and virtual topics, with physical create/delete actually talking to the broker) and flatten the `/config` prefix away (`/rbac/roles`, `/rbac/groups`, `/auth/users`, `/governance`).

**Architecture:** `kawa-http-admin` gains a `TopicAdmin` abstraction (AdminClient-backed `KafkaTopicAdmin`) so the admin layer can create/delete physical topics on the real cluster. `POST /topics` takes an explicit `"type": "physical" | "virtual"` discriminator: virtual bodies write a `VirtualTopicConfig` through the existing `GatewayConfigRepository` (no governance), physical bodies run the existing governance admission check and then create on the broker. `PUT /topics/{name}` upserts virtual topic config; `DELETE /topics/{name}` removes virtual config or deletes the physical topic on the broker. `GetTopicsHandler` is fixed to list virtual topics independently of the physical cache so created virtual topics are always visible. The `/config` prefix is dropped from all config routes; `VirtualTopicsConfigHandler` is deleted (folded into the unified handlers).

**Tech Stack:** Java 26, Netty (existing `Router`/`HttpRouterHandler`), Jackson 3 (`tools.jackson.databind`), `org.apache.kafka:kafka-clients` (version from parent `dependencyManagement`), JUnit 5 + AssertJ, Testcontainers + Awaitility for ITs.

**Spec:** Design decisions from the API review conversation (2026-09-11):
- One `/topics` resource covers both kinds; `GET /topics` lists both (already does, with a `type` discriminator).
- `POST /topics` body carries an explicit `"type": "physical" | "virtual"` discriminator.
- Governance applies to **physical** specs only; virtual topics are config writes and skip evaluation. (Making the CEL environment virtual-topic-aware is tracked separately in issue #12 — NOT tackled here.)
- Physical create/delete actually hit the broker via the admin layer (option 2 — broker connectivity now).
- `/config` prefix dropped everywhere: `/rbac/roles`, `/rbac/groups`, `/auth/users`, `/governance`. `/config/virtual-topics` disappears entirely.
- Placeholder principal `"admin"` in `CreateTopicHandler` stays (admin HTTP auth is a separate open question).

## Global Constraints

- Build with the repo wrapper `./mvnw` (Java 26 via `.sdkmanrc`). **Do NOT pass `-Pca`** — that flag is for the separate `~/dev/bac` projects.
- Never hardcode dependency versions in module `pom.xml` files — `kafka-clients` version comes from the parent `<dependencyManagement>` (`${kafka.version}`).
- `kawa-http-admin` files use **4-space indentation** (match the surrounding files; never introduce tabs).
- Test methods use `// given` / `// when` / `// then` comment blocks and AssertJ.
- Javadoc is Markdown `///` (JEP 467). `var` for obvious right-hand types; explicit type otherwise.
- `*Test` classes run via surefire (`mvn test`); `*IT.java` in `kawa-integration-tests` run via failsafe (`mvn verify -Dit.test=ClassName`). Testcontainers requires Docker.
- Unit-test command: `./mvnw -pl kawa-http-admin -am test`. IT command: `./mvnw -pl kawa-integration-tests -am verify -Dit.test=<Class>`.

---

### Task 1: `TopicAdmin` interface + `KafkaTopicAdmin` (AdminClient) + kafka-clients dependency

**Files:**
- Modify: `kawa-http-admin/pom.xml` (add `kafka-clients` dependency)
- Create: `kawa-http-admin/src/main/java/io/jonasg/kawa/http/TopicAdmin.java`
- Create: `kawa-http-admin/src/main/java/io/jonasg/kawa/http/KafkaTopicAdmin.java`
- Test: `kawa-http-admin/src/test/java/io/jonasg/kawa/http/KafkaTopicAdminTest.java`

**Interfaces:**
- Produces: `TopicAdmin` interface with `void createTopic(TopicSpec spec) throws Exception`, `void deleteTopic(String name) throws Exception`, `void close()`; `KafkaTopicAdmin implements TopicAdmin` with constructor `KafkaTopicAdmin(String bootstrapServers, BrokerAuthConfig brokerAuth)` and package-private static `Properties props(String bootstrapServers, BrokerAuthConfig brokerAuth)`. Later tasks consume `TopicAdmin` (Tasks 3, 5, 6) and `KafkaTopicAdmin` (Task 7).

- [ ] **Step 1: Add the kafka-clients dependency to `kawa-http-admin/pom.xml`**

Add inside `<dependencies>` (after the `kawa-governance` dependency):

```xml
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-clients</artifactId>
    </dependency>
```

No `<version>` — managed by the parent.

- [ ] **Step 2: Write the failing test**

Create `KafkaTopicAdminTest.java`:

```java
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
```

- [ ] **Step 3: Run test to verify it fails**

Run: `./mvnw -pl kawa-http-admin -am test -Dtest=KafkaTopicAdminTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — `KafkaTopicAdmin` does not exist (compile error).

- [ ] **Step 4: Write the minimal implementation**

Create `TopicAdmin.java`:

```java
package io.jonasg.kawa.http;

import io.jonasg.kawa.governance.TopicSpec;

/// Broker topic operations for the admin HTTP surface. Implementations talk to the real
/// cluster; [KafkaTopicAdmin] uses Kafka's AdminClient.
public interface TopicAdmin extends AutoCloseable {

    /// Creates the topic on the broker. Throws when the broker rejects the request
    /// (e.g. `TopicExistsException` when the topic already exists).
    void createTopic(TopicSpec spec) throws Exception;

    /// Deletes the topic on the broker.
    void deleteTopic(String name) throws Exception;

    @Override
    void close();
}
```

Create `KafkaTopicAdmin.java`:

```java
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
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./mvnw -pl kawa-http-admin -am test -Dtest=KafkaTopicAdminTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add kawa-http-admin/pom.xml kawa-http-admin/src/main/java/io/jonasg/kawa/http/TopicAdmin.java kawa-http-admin/src/main/java/io/jonasg/kawa/http/KafkaTopicAdmin.java kawa-http-admin/src/test/java/io/jonasg/kawa/http/KafkaTopicAdminTest.java
git commit -m "feat: add TopicAdmin broker client for the admin HTTP surface"
```

---

### Task 2: `GetTopicsHandler` lists virtual topics independently of the physical cache

**Files:**
- Modify: `kawa-http-admin/src/main/java/io/jonasg/kawa/http/GetTopicsHandler.java`
- Test: `kawa-http-admin/src/test/java/io/jonasg/kawa/http/GetTopicsHandlerTest.java`

**Interfaces:**
- Consumes: `VirtualTopicManager.virtualTopics()` (returns `Map<String, String>` virtual→physical), `VirtualTopicManager.filterFor(String)` (returns `Optional<VirtualTopicFilterConfig>`), `MetadataCache.partitionCount(String)` / `MetadataCache.replicationFactor(String)` / `MetadataCache.topics()`.
- Produces: unchanged `GetTopicsHandler(VirtualTopicManager, MetadataCache)` — but now lists every configured virtual topic (from the manager) even when its physical backing is absent from the cache, followed by every physical topic from the cache. Existing output for virtualized physical topics is unchanged.

- [ ] **Step 1: Write the failing test**

Add to `GetTopicsHandlerTest.java`:

```java
    @Test
    void listsVirtualTopicWithoutPhysicalBacking() {
        // given
        var virtualTopics = new VirtualTopicManager(Map.of(
                "orders", new VirtualTopicConfig("orders-v2")));
        var handler = new GetTopicsHandler(virtualTopics, new MetadataCache());

        // when
        List<TopicView> topics = topics(handler);

        // then
        assertThat(topics).containsExactly(
                new TopicView("virtual", "orders", 0, 0, null, "orders-v2"));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw -pl kawa-http-admin -am test -Dtest=GetTopicsHandlerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — `topics` is empty (current handler only emits virtual views for physical topics present in the cache).

- [ ] **Step 3: Write the minimal implementation**

Replace the body of `GetTopicsHandler.handle` with:

```java
    @Override
    public Router.Response<List<TopicView>> handle(Router.Request request) {
        List<TopicView> views = new ArrayList<>();
        for (Map.Entry<String, String> entry : virtualTopics.virtualTopics().entrySet()) {
            String virtual = entry.getKey();
            String physical = entry.getValue();
            views.add(new TopicView(
                    "virtual",
                    virtual,
                    cache.partitionCount(physical),
                    cache.replicationFactor(physical),
                    toFilterView(virtualTopics.filterFor(virtual).orElse(null)),
                    physical));
        }
        for (TopicMetadata tm : cache.topics()) {
            views.add(new TopicView(
                    "physical",
                    tm.name(),
                    cache.partitionCount(tm.name()),
                    cache.replicationFactor(tm.name()),
                    null,
                    null));
        }
        return Router.Response.ok(views);
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./mvnw -pl kawa-http-admin -am test -Dtest=GetTopicsHandlerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS — all 4 tests (the 3 existing ones still pass because virtualized physical topics produce the same virtual+physical pair).

- [ ] **Step 5: Commit**

```bash
git add kawa-http-admin/src/main/java/io/jonasg/kawa/http/GetTopicsHandler.java kawa-http-admin/src/test/java/io/jonasg/kawa/http/GetTopicsHandlerTest.java
git commit -m "feat: list virtual topics in GET /topics independent of physical cache"
```

---

### Task 3: Unified `POST /topics` — `TopicCreateRequest` + reworked `CreateTopicHandler`

**Files:**
- Create: `kawa-http-admin/src/main/java/io/jonasg/kawa/http/TopicCreateRequest.java`
- Modify: `kawa-http-admin/src/main/java/io/jonasg/kawa/http/CreateTopicHandler.java`
- Create: `kawa-http-admin/src/test/java/io/jonasg/kawa/http/FakeTopicAdmin.java`
- Modify: `kawa-http-admin/src/test/java/io/jonasg/kawa/http/CreateTopicHandlerTest.java`

**Interfaces:**
- Consumes: `TopicAdmin` (Task 1), `GovernancePolicy.exempt(String, String)` / `GovernancePolicy.evaluate(String, String, TopicSpec)` (existing), `GatewayConfigRepository.current()` / `write(GatewayConfig)` (existing), `GatewayConfig.putVirtualTopic(String, VirtualTopicConfig)` (existing), `TopicSpec(String, int, int, Map<String,String>)` (existing).
- Produces: `TopicCreateRequest(String type, String name, Integer partitions, Short replicationFactor, Map<String,String> configs, String topic, VirtualTopicFilterConfig filter, Boolean exposePhysicalTopic)`; `CreateTopicHandler(GovernancePolicy, GatewayConfigRepository, TopicAdmin)`. `POST /topics` with `"type":"virtual"` writes config and returns `201` with the `VirtualTopicConfig`; with `"type":"physical"` runs governance then `topicAdmin.createTopic(spec)` and returns `201` with the `TopicSpec`; `403` on governance violation; `409` when the topic already exists; `400` on invalid body/type/missing fields.

- [ ] **Step 1: Write the failing tests**

Create `FakeTopicAdmin.java`:

```java
package io.jonasg.kawa.http;

import io.jonasg.kawa.governance.TopicSpec;

import java.util.ArrayList;
import java.util.List;

/// In-memory [TopicAdmin] for handler tests: records create/delete calls instead of talking
/// to a broker.
final class FakeTopicAdmin implements TopicAdmin {

    final List<TopicSpec> created = new ArrayList<>();
    final List<String> deleted = new ArrayList<>();
    RuntimeException createError;

    @Override
    public void createTopic(TopicSpec spec) {
        if (createError != null) {
            throw createError;
        }
        created.add(spec);
    }

    @Override
    public void deleteTopic(String name) {
        deleted.add(name);
    }

    @Override
    public void close() {
    }
}
```

Replace `CreateTopicHandlerTest.java` with:

```java
package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GovernanceConfig;
import io.jonasg.kawa.config.GovernanceExemptionConfig;
import io.jonasg.kawa.config.GovernanceRuleConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.governance.TopicSpec;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CreateTopicHandlerTest {

    private static final GovernancePolicy POLICY = new GovernancePolicy(new GovernanceConfig(null, null));

    private static final byte[] PHYSICAL_BODY = ("{\"type\":\"physical\",\"name\":\"orders\",\"partitions\":3,"
            + "\"replicationFactor\":3,\"configs\":{\"cleanup.policy\":\"compact\"}}")
            .getBytes(StandardCharsets.UTF_8);

    @Test
    void postCreatesPhysicalTopicOnBroker() {
        // given
        var topicAdmin = new FakeTopicAdmin();
        var handler = new CreateTopicHandler(POLICY, new FakeGatewayConfigRepository(null), topicAdmin);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("POST", "/topics", Map.of(), PHYSICAL_BODY));

        // then
        assertThat(response.status()).isEqualTo(201);
        assertThat(response.body()).isEqualTo(new TopicSpec("orders", 3, 3, Map.of("cleanup.policy", "compact")));
        assertThat(topicAdmin.created).containsExactly(new TopicSpec("orders", 3, 3, Map.of("cleanup.policy", "compact")));
    }

    @Test
    void postRejectsInvalidBody() {
        // given
        var handler = new CreateTopicHandler(POLICY, new FakeGatewayConfigRepository(null), new FakeTopicAdmin());

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("POST", "/topics", Map.of(), "not json".getBytes(StandardCharsets.UTF_8)));

        // then
        assertThat(response.status()).isEqualTo(400);
    }

    @Test
    void postRejectsUnknownType() {
        // given
        var handler = new CreateTopicHandler(POLICY, new FakeGatewayConfigRepository(null), new FakeTopicAdmin());

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("POST", "/topics", Map.of(),
                        "{\"type\":\"wormhole\",\"name\":\"orders\"}".getBytes(StandardCharsets.UTF_8)));

        // then
        assertThat(response.status()).isEqualTo(400);
    }

    @Test
    void postRejectsMissingName() {
        // given
        var handler = new CreateTopicHandler(POLICY, new FakeGatewayConfigRepository(null), new FakeTopicAdmin());

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("POST", "/topics", Map.of(),
                        "{\"type\":\"physical\",\"partitions\":3}".getBytes(StandardCharsets.UTF_8)));

        // then
        assertThat(response.status()).isEqualTo(400);
    }

    @Test
    void postRejectsTopicViolatingRules() {
        // given
        var governance = new GovernanceConfig(
                Map.of("min-replication",
                        new GovernanceRuleConfig("replication factor must be at least 3", "topic.replicationFactor >= 3")),
                Map.of());
        var topicAdmin = new FakeTopicAdmin();
        var handler = new CreateTopicHandler(new GovernancePolicy(governance),
                new FakeGatewayConfigRepository(null), topicAdmin);
        byte[] body = "{\"type\":\"physical\",\"name\":\"orders\",\"partitions\":3,\"replicationFactor\":1}"
                .getBytes(StandardCharsets.UTF_8);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("POST", "/topics", Map.of(), body));

        // then
        assertThat(response.status()).isEqualTo(403);
        assertThat(response.body()).isEqualTo(Map.of("error",
                "topic 'orders' rejected by governance: [min-replication] replication factor must be at least 3"));
        assertThat(topicAdmin.created).isEmpty();
    }

    @Test
    void postAcceptsExemptTopicAndCreatesOnBroker() {
        // given
        var governance = new GovernanceConfig(
                Map.of("min-replication",
                        new GovernanceRuleConfig("replication factor must be at least 3", "topic.replicationFactor >= 3")),
                Map.of("ops", new GovernanceExemptionConfig("admin", ".*")));
        var topicAdmin = new FakeTopicAdmin();
        var handler = new CreateTopicHandler(new GovernancePolicy(governance),
                new FakeGatewayConfigRepository(null), topicAdmin);
        byte[] body = "{\"type\":\"physical\",\"name\":\"orders\",\"partitions\":3,\"replicationFactor\":1}"
                .getBytes(StandardCharsets.UTF_8);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("POST", "/topics", Map.of(), body));

        // then
        assertThat(response.status()).isEqualTo(201);
        assertThat(topicAdmin.created).hasSize(1);
    }

    @Test
    void postReturnsConflictWhenTopicExists() {
        // given
        var topicAdmin = new FakeTopicAdmin();
        topicAdmin.createError = new TopicExistsException("Topic 'orders' already exists.");
        var handler = new CreateTopicHandler(POLICY, new FakeGatewayConfigRepository(null), topicAdmin);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("POST", "/topics", Map.of(), PHYSICAL_BODY));

        // then
        assertThat(response.status()).isEqualTo(409);
    }

    @Test
    void postCreatesVirtualTopicConfig() {
        // given
        var repository = new FakeGatewayConfigRepository(null);
        var topicAdmin = new FakeTopicAdmin();
        var handler = new CreateTopicHandler(POLICY, repository, topicAdmin);
        byte[] body = "{\"type\":\"virtual\",\"name\":\"orders\",\"topic\":\"orders-v2\"}"
                .getBytes(StandardCharsets.UTF_8);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("POST", "/topics", Map.of(), body));

        // then
        assertThat(response.status()).isEqualTo(201);
        assertThat(response.body()).isEqualTo(new VirtualTopicConfig("orders-v2"));
        assertThat(repository.current().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v2"));
        assertThat(topicAdmin.created).isEmpty();
    }

    @Test
    void postRejectsVirtualTopicWithoutBacking() {
        // given
        var handler = new CreateTopicHandler(POLICY, new FakeGatewayConfigRepository(null), new FakeTopicAdmin());

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("POST", "/topics", Map.of(),
                        "{\"type\":\"virtual\",\"name\":\"orders\"}".getBytes(StandardCharsets.UTF_8)));

        // then
        assertThat(response.status()).isEqualTo(400);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -pl kawa-http-admin -am test -Dtest=CreateTopicHandlerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — `TopicCreateRequest` does not exist; `CreateTopicHandler` constructor signature changed.

- [ ] **Step 3: Write the minimal implementation**

Create `TopicCreateRequest.java`:

```java
package io.jonasg.kawa.http;

import io.jonasg.kawa.config.VirtualTopicFilterConfig;

import java.util.Map;

/// The unified `POST /topics` request body: a `type` discriminator plus the fields of one
/// topic kind. Physical topics carry partitions/replicationFactor/configs; virtual topics
/// carry the physical backing topic and optional filter.
public record TopicCreateRequest(
        String type,
        String name,
        Integer partitions,
        Short replicationFactor,
        Map<String, String> configs,
        String topic,
        VirtualTopicFilterConfig filter,
        Boolean exposePhysicalTopic
) {

    public TopicCreateRequest {
        configs = configs == null ? Map.of() : Map.copyOf(configs);
    }
}
```

Replace `CreateTopicHandler.java` with:

```java
package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.governance.GovernancePolicy;
import io.jonasg.kawa.governance.TopicSpec;
import io.jonasg.kawa.governance.Violation;
import org.apache.kafka.common.errors.TopicExistsException;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.stream.Collectors;

/// Serves `POST /topics`: the unified topic creation surface. A `"type": "virtual"` body
/// writes a virtual topic config through the [GatewayConfigRepository]; a
/// `"type": "physical"` body runs the governance admission check and then creates the topic
/// on the broker through the [TopicAdmin].
public final class CreateTopicHandler implements Router.Handler {

    /// Placeholder principal until the admin HTTP layer has authentication.
    private static final String PRINCIPAL = "admin";
    /// Placeholder service binding for rules that reference `service`.
    private static final String SERVICE = "kawa";

    private final GovernancePolicy governance;
    private final GatewayConfigRepository repository;
    private final TopicAdmin topicAdmin;
    private final JsonMapper mapper = JsonMapper.builder().build();

    public CreateTopicHandler(
            GovernancePolicy governance,
            GatewayConfigRepository repository,
            TopicAdmin topicAdmin
    ) {
        this.governance = governance;
        this.repository = repository;
        this.topicAdmin = topicAdmin;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        if (!"POST".equals(request.method())) {
            return Router.Response.badRequest("unsupported method " + request.method());
        }
        TopicCreateRequest body;
        try {
            body = mapper.readValue(request.body(), TopicCreateRequest.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid topic body: " + e.getMessage());
        }
        if (body.name() == null || body.name().isBlank()) {
            return Router.Response.badRequest("topic name is required");
        }
        return switch (body.type()) {
            case "virtual" -> createVirtual(body);
            case "physical" -> createPhysical(body);
            default -> Router.Response.badRequest("type must be 'physical' or 'virtual'");
        };
    }

    private Router.Response<?> createVirtual(TopicCreateRequest body) {
        if (body.topic() == null || body.topic().isBlank()) {
            return Router.Response.badRequest("virtual topic requires a physical 'topic'");
        }
        var config = new VirtualTopicConfig(
                body.topic(), body.filter(), body.exposePhysicalTopic() != null && body.exposePhysicalTopic());
        GatewayConfig base = repository.current() != null ? repository.current() : GatewayConfig.empty();
        repository.write(base.putVirtualTopic(body.name(), config));
        return Router.Response.created(config);
    }

    private Router.Response<?> createPhysical(TopicCreateRequest body) {
        var spec = new TopicSpec(
                body.name(),
                body.partitions() == null ? -1 : body.partitions(),
                body.replicationFactor() == null ? -1 : body.replicationFactor(),
                body.configs());
        if (governance.exempt(PRINCIPAL, spec.name())) {
            return createOnBroker(spec);
        }
        List<Violation> violations = governance.evaluate(PRINCIPAL, SERVICE, spec);
        if (!violations.isEmpty()) {
            String detail = violations.stream()
                    .map(v -> "[" + v.rule() + "] " + v.message())
                    .collect(Collectors.joining("; "));
            return Router.Response.forbidden(
                    "topic '" + spec.name() + "' rejected by governance: " + detail);
        }
        return createOnBroker(spec);
    }

    private Router.Response<?> createOnBroker(TopicSpec spec) {
        try {
            topicAdmin.createTopic(spec);
            return Router.Response.created(spec);
        } catch (Exception e) {
            if (isTopicExists(e)) {
                return Router.Response.conflict("topic '" + spec.name() + "' already exists");
            }
            return Router.Response.internalError(
                    "failed to create topic '" + spec.name() + "': " + e.getMessage());
        }
    }

    private static boolean isTopicExists(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof TopicExistsException) {
                return true;
            }
        }
        return false;
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./mvnw -pl kawa-http-admin -am test -Dtest=CreateTopicHandlerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS (9 tests).

- [ ] **Step 5: Commit**

```bash
git add kawa-http-admin/src/main/java/io/jonasg/kawa/http/TopicCreateRequest.java kawa-http-admin/src/main/java/io/jonasg/kawa/http/CreateTopicHandler.java kawa-http-admin/src/test/java/io/jonasg/kawa/http/FakeTopicAdmin.java kawa-http-admin/src/test/java/io/jonasg/kawa/http/CreateTopicHandlerTest.java
git commit -m "feat: unify POST /topics with type discriminator and broker creation"
```

---

### Task 4: `PUT /topics/{name}` — `UpdateTopicHandler`

**Files:**
- Create: `kawa-http-admin/src/main/java/io/jonasg/kawa/http/UpdateTopicHandler.java`
- Test: `kawa-http-admin/src/test/java/io/jonasg/kawa/http/UpdateTopicHandlerTest.java`

**Interfaces:**
- Consumes: `GatewayConfigRepository.current()` / `write(GatewayConfig)`, `GatewayConfig.putVirtualTopic(String, VirtualTopicConfig)`, `TopicCreateRequest` (Task 3).
- Produces: `UpdateTopicHandler(GatewayConfigRepository)` handling `PUT /topics/{name}` — upserts the `VirtualTopicConfig` for the path `name` (same semantics as the old `PUT /config/virtual-topics/{name}`), returns `200` with the stored config, `400` on invalid body. **Decision (2026-09-11, option B):** the body uses the same `type` discriminator as `POST /topics` — only `"type": "virtual"` is accepted; `"type": "physical"` and missing `type` return `400` ("only 'virtual' topics can be updated"), and a virtual body without a physical `topic` returns `400`. This keeps the unified surface self-describing and prevents a physical-looking body from being silently stored as a broken virtual config.

- [ ] **Step 1: Write the failing tests**

Create `UpdateTopicHandlerTest.java`:

```java
package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class UpdateTopicHandlerTest {

    @Test
    void putAddsVirtualTopicAndPersistsSnapshot() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty());
        var handler = new UpdateTopicHandler(repository);
        byte[] body = "{\"type\":\"virtual\",\"topic\":\"raw-orders\"}".getBytes(StandardCharsets.UTF_8);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/topics/orders", Map.of("name", "orders"), body));

        // then
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.body()).isEqualTo(new VirtualTopicConfig("raw-orders"));
        assertThat(repository.current().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("raw-orders"));
    }

    @Test
    void putOverwritesExistingEntry() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .putVirtualTopic("orders", new VirtualTopicConfig("raw-old")));
        var handler = new UpdateTopicHandler(repository);
        byte[] body = "{\"type\":\"virtual\",\"topic\":\"raw-new\"}".getBytes(StandardCharsets.UTF_8);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/topics/orders", Map.of("name", "orders"), body));

        // then
        assertThat(response.status()).isEqualTo(200);
        assertThat(repository.current().virtualTopics()).hasSize(1);
        assertThat(repository.current().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("raw-new"));
    }

    @Test
    void putRejectsInvalidBody() {
        // given
        var handler = new UpdateTopicHandler(new FakeGatewayConfigRepository(GatewayConfig.empty()));

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/topics/orders", Map.of("name", "orders"),
                        "not json".getBytes(StandardCharsets.UTF_8)));

        // then
        assertThat(response.status()).isEqualTo(400);
    }

    @Test
    void putRejectsPhysicalType() {
        // given
        var handler = new UpdateTopicHandler(new FakeGatewayConfigRepository(GatewayConfig.empty()));

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/topics/orders", Map.of("name", "orders"),
                        "{\"type\":\"physical\",\"partitions\":5}".getBytes(StandardCharsets.UTF_8)));

        // then
        assertThat(response.status()).isEqualTo(400);
    }

    @Test
    void putRejectsVirtualWithoutBacking() {
        // given
        var handler = new UpdateTopicHandler(new FakeGatewayConfigRepository(GatewayConfig.empty()));

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("PUT", "/topics/orders", Map.of("name", "orders"),
                        "{\"type\":\"virtual\"}".getBytes(StandardCharsets.UTF_8)));

        // then
        assertThat(response.status()).isEqualTo(400);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -pl kawa-http-admin -am test -Dtest=UpdateTopicHandlerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — `UpdateTopicHandler` does not exist.

- [ ] **Step 3: Write the minimal implementation**

Create `UpdateTopicHandler.java`:

```java
package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.VirtualTopicConfig;
import tools.jackson.databind.json.JsonMapper;

/// Serves `PUT /topics/{name}`: upserts the virtual topic config for `name`. The body uses
/// the same `type` discriminator as `POST /topics`; only `"type": "virtual"` is supported
/// (physical topic alteration is not implemented).
public final class UpdateTopicHandler implements Router.Handler {

    private final GatewayConfigRepository repository;
    private final JsonMapper mapper = JsonMapper.builder().build();

    public UpdateTopicHandler(GatewayConfigRepository repository) {
        this.repository = repository;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        if (!"PUT".equals(request.method())) {
            return Router.Response.badRequest("unsupported method " + request.method());
        }
        String name = request.pathParams().get("name");
        TopicCreateRequest body;
        try {
            body = mapper.readValue(request.body(), TopicCreateRequest.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid topic body: " + e.getMessage());
        }
        if (!"virtual".equals(body.type())) {
            return Router.Response.badRequest("only 'virtual' topics can be updated");
        }
        if (body.topic() == null || body.topic().isBlank()) {
            return Router.Response.badRequest("virtual topic requires a physical 'topic'");
        }
        var value = new VirtualTopicConfig(
                body.topic(), body.filter(), body.exposePhysicalTopic() != null && body.exposePhysicalTopic());
        GatewayConfig base = repository.current() != null ? repository.current() : GatewayConfig.empty();
        repository.write(base.putVirtualTopic(name, value));
        return Router.Response.ok(value);
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./mvnw -pl kawa-http-admin -am test -Dtest=UpdateTopicHandlerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add kawa-http-admin/src/main/java/io/jonasg/kawa/http/UpdateTopicHandler.java kawa-http-admin/src/test/java/io/jonasg/kawa/http/UpdateTopicHandlerTest.java
git commit -m "feat: add PUT /topics/{name} for virtual topic config"
```

---

### Task 5: `DELETE /topics/{name}` — `DeleteTopicHandler`

**Files:**
- Create: `kawa-http-admin/src/main/java/io/jonasg/kawa/http/DeleteTopicHandler.java`
- Test: `kawa-http-admin/src/test/java/io/jonasg/kawa/http/DeleteTopicHandlerTest.java`

**Interfaces:**
- Consumes: `GatewayConfigRepository.current()` / `write(GatewayConfig)`, `GatewayConfig.virtualTopics()` / `removeVirtualTopic(String)`, `MetadataCache.topics()`, `TopicAdmin.deleteTopic(String)`.
- Produces: `DeleteTopicHandler(GatewayConfigRepository, MetadataCache, TopicAdmin)` handling `DELETE /topics/{name}` — removes the virtual topic config when `name` is a virtual topic (no broker call), otherwise deletes the physical topic on the broker; `204` on success, `404` when the name is neither, `500` on broker failure. A name that is both virtual and physical resolves to the virtual config removal.

- [ ] **Step 1: Write the failing tests**

Create `DeleteTopicHandlerTest.java`:

```java
package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.VirtualTopicConfig;
import io.jonasg.kawa.core.cluster.BrokerNode;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.cluster.MetadataSnapshot;
import io.jonasg.kawa.core.cluster.PartitionMetadata;
import io.jonasg.kawa.core.cluster.TopicMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DeleteTopicHandlerTest {

    @Test
    void deleteRemovesVirtualTopicConfigWithoutBrokerCall() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .putVirtualTopic("orders", new VirtualTopicConfig("orders-v2")));
        var topicAdmin = new FakeTopicAdmin();
        var handler = new DeleteTopicHandler(repository, new MetadataCache(), topicAdmin);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("DELETE", "/topics/orders", Map.of("name", "orders"), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(204);
        assertThat(repository.current().virtualTopics()).isEmpty();
        assertThat(topicAdmin.deleted).isEmpty();
    }

    @Test
    void deleteDeletesPhysicalTopicOnBroker() {
        // given
        var cache = new MetadataCache();
        cache.update(MetadataSnapshot.of(
                Map.of("orders", TopicMetadata.of("orders",
                        List.of(PartitionMetadata.of(0, 1, List.of(1), List.of(1), List.of())))),
                Map.of(1, BrokerNode.of(1, "localhost", 9092, null)),
                "test-cluster"));
        var topicAdmin = new FakeTopicAdmin();
        var handler = new DeleteTopicHandler(
                new FakeGatewayConfigRepository(GatewayConfig.empty()), cache, topicAdmin);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("DELETE", "/topics/orders", Map.of("name", "orders"), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(204);
        assertThat(topicAdmin.deleted).containsExactly("orders");
    }

    @Test
    void deleteUnknownTopicReturnsNotFound() {
        // given
        var handler = new DeleteTopicHandler(
                new FakeGatewayConfigRepository(GatewayConfig.empty()), new MetadataCache(), new FakeTopicAdmin());

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("DELETE", "/topics/orders", Map.of("name", "orders"), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(404);
    }

    @Test
    void deleteVirtualTopicWinsOverPhysicalName() {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .putVirtualTopic("orders", new VirtualTopicConfig("orders-v2")));
        var cache = new MetadataCache();
        cache.update(MetadataSnapshot.of(
                Map.of("orders", TopicMetadata.of("orders",
                        List.of(PartitionMetadata.of(0, 1, List.of(1), List.of(1), List.of())))),
                Map.of(1, BrokerNode.of(1, "localhost", 9092, null)),
                "test-cluster"));
        var topicAdmin = new FakeTopicAdmin();
        var handler = new DeleteTopicHandler(repository, cache, topicAdmin);

        // when
        Router.Response<?> response = handler.handle(
                new Router.Request("DELETE", "/topics/orders", Map.of("name", "orders"), new byte[0]));

        // then
        assertThat(response.status()).isEqualTo(204);
        assertThat(repository.current().virtualTopics()).isEmpty();
        assertThat(topicAdmin.deleted).isEmpty();
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -pl kawa-http-admin -am test -Dtest=DeleteTopicHandlerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — `DeleteTopicHandler` does not exist.

- [ ] **Step 3: Write the minimal implementation**

Create `DeleteTopicHandler.java`:

```java
package io.jonasg.kawa.http;

import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.core.cluster.MetadataCache;

/// Serves `DELETE /topics/{name}`: removes a virtual topic's config when `name` is a virtual
/// topic, otherwise deletes the physical topic on the broker through the [TopicAdmin].
/// A name that is both virtual and physical resolves to the virtual config removal (the
/// safer operation - no broker data loss).
public final class DeleteTopicHandler implements Router.Handler {

    private final GatewayConfigRepository repository;
    private final MetadataCache cache;
    private final TopicAdmin topicAdmin;

    public DeleteTopicHandler(GatewayConfigRepository repository, MetadataCache cache, TopicAdmin topicAdmin) {
        this.repository = repository;
        this.cache = cache;
        this.topicAdmin = topicAdmin;
    }

    @Override
    public Router.Response<?> handle(Router.Request request) {
        if (!"DELETE".equals(request.method())) {
            return Router.Response.badRequest("unsupported method " + request.method());
        }
        String name = request.pathParams().get("name");
        GatewayConfig base = repository.current() != null ? repository.current() : GatewayConfig.empty();
        if (base.virtualTopics().containsKey(name)) {
            repository.write(base.removeVirtualTopic(name));
            return Router.Response.noContent();
        }
        boolean physicalExists = cache.topics().stream().anyMatch(t -> t.name().equals(name));
        if (!physicalExists) {
            return Router.Response.notFound("topic '" + name + "' not found");
        }
        try {
            topicAdmin.deleteTopic(name);
            return Router.Response.noContent();
        } catch (Exception e) {
            return Router.Response.internalError("failed to delete topic '" + name + "': " + e.getMessage());
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./mvnw -pl kawa-http-admin -am test -Dtest=DeleteTopicHandlerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add kawa-http-admin/src/main/java/io/jonasg/kawa/http/DeleteTopicHandler.java kawa-http-admin/src/test/java/io/jonasg/kawa/http/DeleteTopicHandlerTest.java
git commit -m "feat: add DELETE /topics/{name} for virtual config and physical broker topics"
```

---

### Task 6: Wire `AdminHttpServer` — unified `/topics` routes, flattened config paths, `KafkaGateway` passes `TopicAdmin`

**Files:**
- Modify: `kawa-http-admin/src/main/java/io/jonasg/kawa/http/AdminHttpServer.java`
- Modify: `kawa-server/src/main/java/io/jonasg/kawa/server/KafkaGateway.java`
- Delete: `kawa-http-admin/src/main/java/io/jonasg/kawa/http/VirtualTopicsConfigHandler.java`
- Delete: `kawa-http-admin/src/test/java/io/jonasg/kawa/http/VirtualTopicsConfigHandlerTest.java`
- Modify: `kawa-http-admin/src/test/java/io/jonasg/kawa/http/AdminHttpServerTest.java`

**Interfaces:**
- Consumes: `CreateTopicHandler(GovernancePolicy, GatewayConfigRepository, TopicAdmin)` (Task 3), `UpdateTopicHandler(GatewayConfigRepository)` (Task 4), `DeleteTopicHandler(GatewayConfigRepository, MetadataCache, TopicAdmin)` (Task 5), `KafkaTopicAdmin(String, BrokerAuthConfig)` (Task 1).
- Produces: `AdminHttpServer(AdminConfig, VirtualTopicManager, MetadataCache, GatewayConfigRepository, GovernancePolicy, TopicAdmin)` — new `TopicAdmin` parameter; `stop()` closes it. Route table: `/topics` GET/POST, `/topics/{name}` PUT/DELETE, `/rbac/roles`, `/rbac/groups`, `/auth/users`, `/governance` (no `/config` prefix). `KafkaGateway.start()` constructs `new KafkaTopicAdmin(bootstrapServers, config.auth().brokerAuth())` and passes it when admin is enabled.

- [ ] **Step 1: Write the failing tests**

Update `AdminHttpServerTest.java`:

1. Add a `FakeTopicAdmin` field and pass it to every `new AdminHttpServer(...)` call (7 existing call sites). The constructor gains a 6th argument — append `, new FakeTopicAdmin()` to each.
2. Update the two path-bearing tests:
   - `configuresAuthUserOverHttp`: `/config/auth/users/alice` → `/auth/users/alice`, `/config/auth/users` → `/auth/users`
   - `servesGovernanceConfigOverHttp`: `/config/governance` → `/governance` (both PUT and GET)
   - `rejectsTopicOverHttpWhenGovernanceRuleViolated`: body gains `"type":"physical"` → `{"type":"physical","name":"orders","partitions":3,"replicationFactor":1}`
3. Add these new tests:

```java
    @Test
    void createsPhysicalTopicOverHttp() throws Exception {
        // given
        var topicAdmin = new FakeTopicAdmin();
        server = new AdminHttpServer(new AdminConfig(true, "127.0.0.1", 0, null),
                new VirtualTopicManager(Map.of()), new MetadataCache(),
                new FakeGatewayConfigRepository(GatewayConfig.empty()),
                new GovernancePolicy(new GovernanceConfig(null, null)), topicAdmin);
        server.start();

        // when
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + "/topics"))
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"type\":\"physical\",\"name\":\"orders\",\"partitions\":3,\"replicationFactor\":3}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then
        assertThat(response.statusCode()).isEqualTo(201);
        assertThat(topicAdmin.created).hasSize(1);
    }

    @Test
    void createsVirtualTopicOverHttp() throws Exception {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty());
        server = new AdminHttpServer(new AdminConfig(true, "127.0.0.1", 0, null),
                new VirtualTopicManager(Map.of()), new MetadataCache(), repository,
                new GovernancePolicy(new GovernanceConfig(null, null)), new FakeTopicAdmin());
        server.start();

        // when
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + "/topics"))
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"type\":\"virtual\",\"name\":\"orders\",\"topic\":\"orders-v2\"}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then
        assertThat(response.statusCode()).isEqualTo(201);
        assertThat(repository.current().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v2"));
    }

    @Test
    void updatesVirtualTopicOverHttp() throws Exception {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty());
        server = new AdminHttpServer(new AdminConfig(true, "127.0.0.1", 0, null),
                new VirtualTopicManager(Map.of()), new MetadataCache(), repository,
                new GovernancePolicy(new GovernanceConfig(null, null)), new FakeTopicAdmin());
        server.start();

        // when
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + "/topics/orders"))
                        .PUT(HttpRequest.BodyPublishers.ofString("{\"type\":\"virtual\",\"topic\":\"orders-v2\"}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(repository.current().virtualTopics())
                .containsEntry("orders", new VirtualTopicConfig("orders-v2"));
    }

    @Test
    void deletesPhysicalTopicOverHttp() throws Exception {
        // given
        var topicAdmin = new FakeTopicAdmin();
        MetadataCache cache = new MetadataCache();
        cache.update(MetadataSnapshot.of(
                Map.of("orders", TopicMetadata.of("orders",
                        List.of(PartitionMetadata.of(0, 1, List.of(1), List.of(1), List.of())))),
                Map.of(1, BrokerNode.of(1, "localhost", 9092, null)),
                "test-cluster"));
        server = new AdminHttpServer(new AdminConfig(true, "127.0.0.1", 0, null),
                new VirtualTopicManager(Map.of()), cache,
                new FakeGatewayConfigRepository(GatewayConfig.empty()),
                new GovernancePolicy(new GovernanceConfig(null, null)), topicAdmin);
        server.start();

        // when
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + "/topics/orders"))
                        .DELETE()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then
        assertThat(response.statusCode()).isEqualTo(204);
        assertThat(topicAdmin.deleted).containsExactly("orders");
    }

    @Test
    void deletesVirtualTopicOverHttp() throws Exception {
        // given
        var repository = new FakeGatewayConfigRepository(GatewayConfig.empty()
                .putVirtualTopic("orders", new VirtualTopicConfig("orders-v2")));
        server = new AdminHttpServer(new AdminConfig(true, "127.0.0.1", 0, null),
                new VirtualTopicManager(Map.of()), new MetadataCache(), repository,
                new GovernancePolicy(new GovernanceConfig(null, null)), new FakeTopicAdmin());
        server.start();

        // when
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.boundPort() + "/topics/orders"))
                        .DELETE()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then
        assertThat(response.statusCode()).isEqualTo(204);
        assertThat(repository.current().virtualTopics()).isEmpty();
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -pl kawa-http-admin -am test -Dtest=AdminHttpServerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — `AdminHttpServer` constructor has no `TopicAdmin` parameter (compile error).

- [ ] **Step 3: Write the minimal implementation**

Modify `AdminHttpServer.java`:

1. Add a `private final TopicAdmin topicAdmin;` field.
2. Change the constructor to accept `TopicAdmin topicAdmin` as the 6th parameter and assign it.
3. Replace the router chain with:

```java
        this.router = new Router()
                .get("/topics", new GetTopicsHandler(virtualTopics, cache))
                .post("/topics", new CreateTopicHandler(governance, configRepository, topicAdmin))
                .put("/topics/{name}", new UpdateTopicHandler(configRepository))
                .delete("/topics/{name}", new DeleteTopicHandler(configRepository, cache, topicAdmin))
                .get("/rbac/roles", new RbacRolesConfigHandler(configRepository))
                .put("/rbac/roles/{name}", new RbacRolesConfigHandler(configRepository))
                .delete("/rbac/roles/{name}", new RbacRolesConfigHandler(configRepository))
                .get("/rbac/groups", new RbacGroupsConfigHandler(configRepository))
                .put("/rbac/groups/{name}", new RbacGroupsConfigHandler(configRepository))
                .delete("/rbac/groups/{name}", new RbacGroupsConfigHandler(configRepository))
                .get("/auth/users", new AuthUsersConfigHandler(configRepository))
                .put("/auth/users/{name}", new AuthUsersConfigHandler(configRepository))
                .delete("/auth/users/{name}", new AuthUsersConfigHandler(configRepository))
                .get("/governance", new GovernanceConfigHandler(configRepository, governance))
                .put("/governance", new GovernanceConfigHandler(configRepository, governance));
```

4. In `stop()`, close the topic admin after the server channel:

```java
    public void stop() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
        topicAdmin.close();
    }
```

5. Update the class javadoc to drop the "never talks to the broker" claim (topic create/delete now do):

```java
/// Netty HTTP server exposing the gateway's admin/UI surface: `GET /topics` reads the
/// [VirtualTopicManager] and [MetadataCache], `POST /topics` and `DELETE /topics/{name}`
/// create/delete physical topics on the broker through the [TopicAdmin], and the
/// `/rbac/...`, `/auth/...` and `/governance` endpoints read and write the dynamic config
/// through the [GatewayConfigRepository]. Config writes go to the config topic and are
/// applied by the consumer.
```

Modify `KafkaGateway.java` — replace the admin wiring in `start()`:

```java
        if (config.admin().enabled()) {
            var topicAdmin = new KafkaTopicAdmin(bootstrapServers, config.auth().brokerAuth());
            adminServer = new AdminHttpServer(config.admin(), virtualTopics, cache, dynamicConfig, governance, topicAdmin);
            adminServer.start();
        }
```

Add the import `io.jonasg.kawa.http.KafkaTopicAdmin;` to `KafkaGateway.java`.

Delete `VirtualTopicsConfigHandler.java` and `VirtualTopicsConfigHandlerTest.java`:

```bash
rm kawa-http-admin/src/main/java/io/jonasg/kawa/http/VirtualTopicsConfigHandler.java kawa-http-admin/src/test/java/io/jonasg/kawa/http/VirtualTopicsConfigHandlerTest.java
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./mvnw -pl kawa-http-admin -am test`
Expected: PASS — all kawa-http-admin tests (AdminHttpServerTest now 12 tests; VirtualTopicsConfigHandlerTest removed).

Run: `./mvnw -pl kawa-server -am test`
Expected: PASS — KafkaGateway compiles with the new constructor.

- [ ] **Step 5: Commit**

```bash
git add kawa-http-admin/src/main/java/io/jonasg/kawa/http/AdminHttpServer.java kawa-http-admin/src/test/java/io/jonasg/kawa/http/AdminHttpServerTest.java kawa-server/src/main/java/io/jonasg/kawa/server/KafkaGateway.java
git rm kawa-http-admin/src/main/java/io/jonasg/kawa/http/VirtualTopicsConfigHandler.java kawa-http-admin/src/test/java/io/jonasg/kawa/http/VirtualTopicsConfigHandlerTest.java
git commit -m "feat: wire unified /topics routes and flatten /config paths in admin API"
```

---

### Task 7: Docs — `configuration.md` admin API section

**Files:**
- Modify: `docs/docs/configuration.md`

**Interfaces:**
- Consumes: the final route table from Task 6.

- [ ] **Step 1: Update the Admin API section**

In `docs/docs/configuration.md`, replace the `### GET /topics` + `### Config endpoints` + `### POST /topics` sections (lines ~485-545) with:

```markdown
### `GET /topics`

Lists the virtual and physical topics known to the gateway (see [Admin](#admin)). Virtual
topics are listed even when their physical backing topic does not exist yet.

### `POST /topics`

The unified topic creation surface. The request body carries a `type` discriminator plus
the fields of one topic kind:

```json
{"type": "physical", "name": "orders", "partitions": 3, "replicationFactor": 3, "configs": {"cleanup.policy": "compact"}}
{"type": "virtual", "name": "orders", "topic": "orders-v2", "filter": {"kind": "header", "header": "tenant", "value": "acme"}}
```

A `physical` topic runs the governance admission check and then creates the topic on the
broker: an exempt principal + topic pair is admitted without evaluation, otherwise every
rule must pass. A compliant topic returns `201` with the stored spec; a topic that violates
one or more rules returns `403` with the violation messages; a topic that already exists
returns `409`. A `virtual` topic writes the virtual topic config (no governance applies)
and returns `201` with the stored config. An invalid body, unknown `type`, or a virtual
topic without a physical `topic` returns `400`.

The admin HTTP layer has no authentication yet, so the requesting principal is a
placeholder (`admin`) until real admin auth lands.

### `PUT /topics/{name}`

Adds or replaces the virtual topic config for `name`. The body uses the same `type`
discriminator as `POST /topics` — only `"type": "virtual"` is accepted
(`{"type": "virtual", "topic": "orders-v2", "filter": {...}}`). Returns `200` with the
stored config; an invalid body, a `"type": "physical"` body, or a virtual body without a
physical `topic` returns `400`. Physical topic alteration is not supported.

### `DELETE /topics/{name}`

Removes a topic. When `name` is a virtual topic, its config is removed (no broker
operation). Otherwise the physical topic is deleted on the broker. Returns `204`; a name
that is neither virtual nor physical returns `404`. A name that is both resolves to the
virtual config removal.

### Config endpoints

The `/rbac/...`, `/auth/...` and `/governance` endpoints read and write the dynamic config
(RBAC, client auth and governance). `GET` lists a section, `PUT /{name}` upserts one entry,
`DELETE /{name}` removes it. Writes persist a full snapshot to the config topic and are
applied live.

| Endpoint | Method | Description |
|---|---|---|
| `/rbac/roles` | GET | List roles |
| `/rbac/roles/{name}` | PUT | Add or replace a role |
| `/rbac/roles/{name}` | DELETE | Remove a role |
| `/rbac/groups` | GET | List groups |
| `/rbac/groups/{name}` | PUT | Add or replace a group |
| `/rbac/groups/{name}` | DELETE | Remove a group |
| `/auth/users` | GET | List users |
| `/auth/users/{name}` | PUT | Add or replace a user |
| `/auth/users/{name}` | DELETE | Remove a user |
| `/governance` | GET | List the governance section (rules + exemptions) |
| `/governance` | PUT | Replace the whole governance section |

The request body for a `PUT` is the entry's JSON object, using the same fields as the
YAML reference above — e.g. `{"acls": [...]}` for a role, `{"members": [...], "roles": [...]}`
for a group, `{"mechanism": "PLAIN", "password": "..."}` for a user, or the full
`{"topicRules": {...}, "exemptions": {...}}` object for governance.

Adding a user via `PUT /auth/users/{name}` auto-expands the advertised SASL
mechanisms to include the user's mechanism, so the first user can be added to an empty
config. The user's `mechanism` is required — a `PUT` without it is rejected with `400`.

Unlike the per-entry sections, `PUT /governance` replaces the **whole**
governance section in one write. Every rule expression is validated before the snapshot
is persisted — a `PUT` containing an invalid CEL expression is rejected with `400` and
nothing is written.

`PUT` returns `200` with the stored entry, `DELETE` returns `204`, a missing entry on
`DELETE` returns `404`, and an invalid body returns `400`.
```

Also update the "Dynamic config" section if it references `/config/...` paths (search for
`/config/` in the file and update any remaining references to the flattened paths).

- [ ] **Step 2: Verify the docs build**

Run: `npm run build` in `docs/`
Expected: SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add docs/docs/configuration.md
git commit -m "docs: document unified /topics admin API and flattened config paths"
```

---

### Task 8: Integration tests — update paths, add `TopicsAdminApiIT`

**Files:**
- Modify: `kawa-integration-tests/src/test/java/io/jonasg/kawa/it/AdminConfigApiIT.java`
- Modify: `kawa-integration-tests/src/test/java/io/jonasg/kawa/it/GovernanceAdminApiIT.java`
- Create: `kawa-integration-tests/src/test/java/io/jonasg/kawa/it/TopicsAdminApiIT.java`

**Interfaces:**
- Consumes: the final route table from Task 6; `KafkaGateway.adminBoundPort()` (existing).

- [ ] **Step 1: Update the existing ITs' paths**

In `AdminConfigApiIT.java`, replace the three request paths:
- `/config/auth/users/alice` → `/auth/users/alice`
- `/config/rbac/roles/allow-all` → `/rbac/roles/allow-all`
- `/config/rbac/groups/admins` → `/rbac/groups/admins`

In `GovernanceAdminApiIT.java`, replace both `/config/governance` paths (PUT and GET) with `/governance`.

- [ ] **Step 2: Write the failing IT**

Create `TopicsAdminApiIT.java`:

```java
package io.jonasg.kawa.it;

import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.config.AdminConfig;
import io.jonasg.kawa.config.AuthConfig;
import io.jonasg.kawa.config.ClusterConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.ListenerConfig;
import io.jonasg.kawa.config.MetricsConfig;
import io.jonasg.kawa.server.KafkaGateway;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end flow for the unified `/topics` admin surface: physical topics are created and
/// deleted on the real broker, virtual topics are created/updated/removed as gateway config,
/// and `GET /topics` lists both.
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TopicsAdminApiIT {

    static final String CONFIG_TOPIC = "__kawa";

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    private static KafkaGateway gateway;
    private static AdminClient brokerAdmin;

    @BeforeAll
    void setUp() throws Exception {
        String brokerBootstrap = kafka.getBootstrapServers();
        brokerAdmin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, brokerBootstrap));
        brokerAdmin.createTopics(List.of(new NewTopic(CONFIG_TOPIC, 1, (short) 1)
                .configs(Map.of("cleanup.policy", "compact")))).all().get();

        var bootstrap = new GatewayConfig(
                "test-gateway",
                List.of(new ListenerConfig("127.0.0.1", 0)),
                Map.of("default", new ClusterConfig("default", List.of(brokerBootstrap))),
                null,
                new AdvertisedListener(1, "localhost", 0),
                new MetricsConfig(false, 0),
                new AuthConfig(null, null, null),
                null,
                new AdminConfig(true, "127.0.0.1", 0, null),
                CONFIG_TOPIC, null);

        gateway = new KafkaGateway(bootstrap);
        gateway.start();
    }

    @AfterAll
    void tearDown() {
        if (brokerAdmin != null) {
            brokerAdmin.close();
        }
        if (gateway != null) {
            gateway.stop();
        }
    }

    @Test
    void createsAndDeletesPhysicalTopicViaAdminApi() throws Exception {
        // given - a fresh gateway with the admin HTTP server enabled
        String base = "http://127.0.0.1:" + gateway.adminBoundPort();
        var http = HttpClient.newHttpClient();

        // when - a physical topic is created through the unified /topics surface
        HttpResponse<String> create = http.send(
                HttpRequest.newBuilder(URI.create(base + "/topics"))
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"type\":\"physical\",\"name\":\"orders\",\"partitions\":3,\"replicationFactor\":1}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then - the topic exists on the broker
        assertThat(create.statusCode()).isEqualTo(201);
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() ->
                        assertThat(brokerAdmin.listTopics().names().get()).contains("orders"));

        // when - the topic is deleted through the unified /topics surface
        HttpResponse<String> delete = http.send(
                HttpRequest.newBuilder(URI.create(base + "/topics/orders"))
                        .DELETE()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then - the topic is gone from the broker
        assertThat(delete.statusCode()).isEqualTo(204);
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() ->
                        assertThat(brokerAdmin.listTopics().names().get()).doesNotContain("orders"));
    }

    @Test
    void createsUpdatesAndDeletesVirtualTopicViaAdminApi() throws Exception {
        // given - a fresh gateway with the admin HTTP server enabled
        String base = "http://127.0.0.1:" + gateway.adminBoundPort();
        var http = HttpClient.newHttpClient();

        // when - a virtual topic is created through the unified /topics surface
        HttpResponse<String> create = http.send(
                HttpRequest.newBuilder(URI.create(base + "/topics"))
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"type\":\"virtual\",\"name\":\"orders\",\"topic\":\"orders-v2\"}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then - the virtual topic is listed by GET /topics once the consumer applies it
        assertThat(create.statusCode()).isEqualTo(201);
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    HttpResponse<String> list = http.send(
                            HttpRequest.newBuilder(URI.create(base + "/topics")).GET().build(),
                            HttpResponse.BodyHandlers.ofString());
                    assertThat(list.body()).contains("\"type\":\"virtual\"", "\"name\":\"orders\"");
                });

        // when - the virtual topic config is updated
        HttpResponse<String> update = http.send(
                HttpRequest.newBuilder(URI.create(base + "/topics/orders"))
                        .PUT(HttpRequest.BodyPublishers.ofString("{\"type\":\"virtual\",\"topic\":\"orders-v3\"}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then - the update is persisted
        assertThat(update.statusCode()).isEqualTo(200);

        // when - the virtual topic is deleted
        HttpResponse<String> delete = http.send(
                HttpRequest.newBuilder(URI.create(base + "/topics/orders"))
                        .DELETE()
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // then - the virtual topic is no longer listed
        assertThat(delete.statusCode()).isEqualTo(204);
        HttpResponse<String> list = http.send(
                HttpRequest.newBuilder(URI.create(base + "/topics")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(list.body()).doesNotContain("\"name\":\"orders\"");
    }
}
```

- [ ] **Step 3: Run the ITs to verify they pass**

Run: `./mvnw -pl kawa-integration-tests -am verify -Dit.test=TopicsAdminApiIT,AdminConfigApiIT,GovernanceAdminApiIT -DfailIfNoTests=false`
Expected: PASS — all three ITs (Docker must be running).

- [ ] **Step 4: Full reactor verification**

Run: `./mvnw test`
Expected: BUILD SUCCESS (all unit tests; ITs are skipped in surefire).

- [ ] **Step 5: Commit**

```bash
git add kawa-integration-tests/src/test/java/io/jonasg/kawa/it/AdminConfigApiIT.java kawa-integration-tests/src/test/java/io/jonasg/kawa/it/GovernanceAdminApiIT.java kawa-integration-tests/src/test/java/io/jonasg/kawa/it/TopicsAdminApiIT.java
git commit -m "test: cover unified /topics admin API end-to-end"
```

---

## Self-Review

**Spec coverage:**
- One `/topics` resource (GET/POST/PUT/DELETE) — Tasks 2-6.
- `type` discriminator on POST — Task 3.
- Governance applies to physical only; virtual skips — Task 3 (`createVirtual` writes config without touching governance; issue #12 tracks the follow-up).
- Physical create/delete hit the broker — Tasks 1, 3, 5.
- `/config` prefix dropped everywhere; `/config/virtual-topics` disappears — Task 6 (route table) + Task 7 (docs) + Task 8 (ITs).
- `GET /topics` lists virtual topics even without a physical backing — Task 2 (needed so created virtual topics are visible).

**Placeholder scan:** No TBD/TODO; every task has concrete code and expected test output.

**Type consistency:** `TopicAdmin` (Task 1) is consumed by `CreateTopicHandler` (Task 3), `DeleteTopicHandler` (Task 5), `AdminHttpServer` (Task 6), `KafkaGateway` (Task 6), and `FakeTopicAdmin` (Task 3). `TopicCreateRequest` (Task 3) is only used by `CreateTopicHandler`. `UpdateTopicHandler`/`DeleteTopicHandler` constructors match their Task 6 wiring. `KafkaTopicAdmin.props` is package-private static, tested in Task 1 and used by the public constructor.