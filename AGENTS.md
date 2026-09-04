# AGENTS.md

Kafka Access Gateway (`kawa`): a Netty-based gateway that terminates Kafka
client connections and enforces RBAC/ACLs before forwarding requests to a real
Kafka cluster. Maven multi-module, Java 26.

## Build & run

- Build with the repo wrapper: `./mvnw` (failsafe/surefire logs land under
  `target/`).
- **The JVM must be Java 26.** `pom.xml` sets `maven.compiler.release=26`.
  The repo pins the JDK via `.sdkmanrc` (`java=26-tem`) — SDKMAN auto-env loads
  it on entering the repo (enable `sdkman_auto_env` in `~/.sdkmanrc`). The
  default `JAVA_HOME` is Java 21 and fails with
  `class file version 70.0 ... only recognizes ... up to 65.0`, so if auto-env
  doesn't apply, set it explicitly:
  `JAVA_HOME=$HOME/.sdkman/candidates/java/26-tem`
- **Do NOT pass `-Pca`** — that flag is specific to the separate `~/dev/bac`
  projects and doesn't exist in this repo.
- Single module + its upstream deps:
  `./mvnw -pl <module> -am test` (with the Java 26 JDK active via `.sdkmanrc`).
- Single test class:
  `./mvnw -pl <module> -am test -Dtest=ClassName -Dsurefire.failIfNoSpecifiedTests=false`

## Test split (easy to get wrong)

- `*Test` classes (in `gateway-*` modules) run via **surefire** — `mvn test`.
- `kawa-integration-tests/src/test/java/.../*IT.java` run via **failsafe**, NOT
  surefire (surefire is `skipTests`-disabled there). Use `verify` and filter
  with `-Dit.test=ClassName` (not `-Dtest=`) to run a single IT.
- The integration tests use **Testcontainers** (Kafka container) — Docker must
  be running; they are slow and network/docker dependent.

## Modules (dependency direction)

`kawa-config` → `kawa-core` ↔ `kawa-protocol-kafka` →
`kawa-virtual-topic`, `kawa-rbac`, `kawa-http-admin` → `kawa-server` → `kawa-integration-tests`.

- `kawa-config`: `GatewayConfig`/`ResourceConfig`/`RbacConfig` + YAML
  `ConfigLoader` (plain Jackson; no custom `ResourceType` deserializer).
- `kawa-core`: `GatewayContext`, interceptors, `VirtualTopicManager`.
- `kawa-protocol-kafka`: `KafkaApiRegistry` — the decoded API/version table;
  APIs not registered pass through ungated.
- `kawa-rbac`: `AuthorizationInterceptor` + per-API `AuthorizationCheck`s.
  One check per gate shape; RBAC is **unconditional** (no opt-out) and
  **default-deny**.
- `kawa-http-admin`: admin HTTP surface (e.g. `GET /topics`) exposing gateway
  state to a UI; reads `VirtualTopicManager`/`MetadataCache`, never talks to the broker.
- `kawa-server`: Netty server, broker clients, shading. Produces the runnable
  shaded jar (`mainClass=io.jonasg.kawa.server.GatewayLauncher`).
- `kawa-integration-tests`: real-client + raw-socket wire tests against a broker via
  `GatewayTestSupport`.

## Code style (repo-specific)

- Test methods use `// given` / `// when` / `// then` comment blocks and AssertJ.
- Javadoc is Markdown `///` (Java 23+/JEP 467). `var` for obvious right-hand
  types; explicit type otherwise.
- **Indentation is inconsistent across files** (most use tabs, some 4-space,
  several already violate both). Match the surrounding file — never introduce
  tabs into a 4-space file or vice versa.
- `GatewayTestSupport` (kawa-integration-tests) is the shared lifecycle; subclasses
  override `authConfig()`/`rbacConfig()`/`initialTopics()`.
- Raw-socket wire tests: see `VirtualTopicsIT.RawFetchSession` (a reusable
  hand-rolled request reader that resolves `localhost` via
  `InetAddress.getAllByName` + try-each-address; prefer that loop over
  `new Socket(host, port)` to avoid multi-address flakiness).

## Docs

`docs/` is a separate Docusaurus site (Node ≥ 20): `npm run start` (dev),
`npm run build`, `npm run typecheck`. Authoritative concept pages live in
`docs/docs/concepts/` (`architecture.md`, `authentication.md`, `rbac.md`,
`virtual-topics.md`). Keep the "What is enforced today" RBAC table in
`rbac.md` in sync with `kawa-rbac`'s `AuthorizationInterceptor` registry.