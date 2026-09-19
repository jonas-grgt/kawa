# kawa-http-admin agent instructions

## Scope

These instructions apply to the `kawa-http-admin` module only.

## Handler naming

Name HTTP handlers after the HTTP method they own: `Get`, `Post`, `Put`, `Patch`, or `Delete`,
followed by the resource name. Use singular or plural resource names according to the route's
resource shape. Keep handlers method-specific rather than combining multiple HTTP methods in one
class.

## HTTP tests

- HTTP slice tests must exercise the real `AdminHttpServer` through an ephemeral port, following
  `AdminHttpSliceTestBase`.
- Assert the JSON wire format and HTTP status codes consumed by the admin UI.
- Use `json-unit`'s AssertJ integration (`assertThatJson`) for structured JSON response assertions instead of asserting
  individual JSON fragments with
  `contains`.
- Use `IGNORING_ARRAY_ORDER` when collection ordering is not part of the behavior under test; do not ignore extra fields
  unless the test explicitly permits them.
- Use `// given`, `// when`, and `// then` sections with AssertJ assertions.
- Write multiline JSON request bodies as Java text blocks (`"""..."""`), not concatenated string literals.
- Keep handler-only tests for pure handler behavior; do not replace slice coverage when testing routing, serialization,
  server bootstrap, or CORS.
- Use `Req` and `Resp` as suffixes for paired request/response variables and fixtures and use java `var`:

  ```java
  var createTopicReq = request("POST", "/topics", body);
  var createTopicResp = send(createTopicReq);
  ```

- Use operation-specific names such as `createTopicReq`, `createTopicResp`,
  `deleteClientReq`, and `deleteClientResp`.
- For a simple one-shot request where constructing a request object adds no value, a descriptive response variable such
  as `topicsResp` is sufficient.
- Prefer `request` and `response` in production APIs and framework types;
  `Req`/`Resp` is a test naming convention, not an API naming convention.

## Verification

- Unit and HTTP slice tests use Surefire and can be run with:

  ```text
  ./mvnw -pl kawa-http-admin -am test
  ```

- Keep HTTP slice tests separate from the pure plumbing tests such as
  `RouterTest` and `KafkaTopicAdminTest`.

## Style

- Match the surrounding file's indentation and formatting.
- Prefer small, behavior-focused tests with descriptive test method names.
- Do not introduce a new abstraction when an existing helper or base class already covers the use case.
