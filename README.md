# Kafka Connect Registry Validation SMT

A Kafka Connect Single Message Transform that validates each record value against the **JSON Schema registered in
Confluent Schema Registry** and reroutes invalid records to a dead letter topic, instead of letting the converter drop
them.

Source connectors have no dead letter queue: with `errors.tolerance=all`, a record rejected by `JsonSchemaConverter`
(`json.fail.invalid.schema=true`) is only logged and lost. This SMT runs the same validation one step earlier, while
the record can still be rerouted.

It works with any source connector producing `Struct` values with a schema (FilePulse, JDBC, …) on topics whose
subject is a JSON Schema.

## How it works

For every record whose value is a `Struct`:

1. Removes the raw field (default `message`, disabled with an empty `raw.field`) from the value.
2. Converts the value to JSON with `JsonSchemaData`, exactly as `JsonSchemaConverter` does.
3. Validates it against the latest schema of `<topic>-value` (or the configured `subject`).
4. **Valid** → forwarded on the same topic, without the raw field.
   **Invalid** → sent to the dead letter topic (`dlq.topic`, or `<topic>.dlq` by default).

All validation rules come from the registered schema: required fields, nested objects, types, patterns, `minItems`…
No field list has to be maintained in the connector configuration. Schemas are cached and refreshed every
`schema.cache.ttl.ms`.

Records already on the dead letter topic are left untouched: the topic equal to `dlq.topic` when it is set, otherwise
any topic ending with `dlq.topic.suffix`. This lets an upstream step (such as a FilePulse failure chain) write to the
same dead letter topic. Schema Registry errors fail the record (they are not routed to the DLQ). Values that are not a
`Struct` (schemaless maps, strings) are forwarded unvalidated, with a warning logged once per topic.

### Dead letter records

Every DLQ record keeps the original key and headers, and gets these headers (prefix `dlq.header.prefix`):

| Header | Example |
|---|---|
| `dlq.error.message` | `#/items/0/amount: expected type: string, actual: number` |
| `dlq.error.subject` | `dev.orders-value` |
| `dlq.original.topic` | `dev.orders` |
| `connect.source.partition` | `{"table":"orders"}` (only with `source.position.headers=true`) |
| `connect.source.offset` | `{"incrementing":7}` (only with `source.position.headers=true`) |

The value depends on `dlq.value.mode`:

- **`envelope`** (default) — a structure holding the raw line (or the record as JSON when there is no raw field) and the
  error. The DLQ subject must accept it:

  ```json
  {
    "message": "<raw line>",
    "error": "#/items/0/amount: expected type: string, actual: number",
    "source": "orders",
    "fileName": "orders_2026.zip",
    "lineNumber": 5
  }
  ```

  `fileName` and `lineNumber` are copied from the `file.name.header` and `line.number.header` headers when present.

- **`original`** — the rejected record unchanged (including the raw field). Nothing source-specific is involved: the
  error is in the headers, the origin is in whatever headers the source connector added. Since the record does not
  match the business schema, the DLQ subject must accept any object, e.g. `{"type": "object"}`.

## Installation

The SMT relies on the classes of the Confluent JSON Schema converter. Copy the jar into the **lib directory of the
`kafka-connect-json-schema-converter` plugin**, so both share the same plugin class loader:

```dockerfile
ADD https://github.com/adiab1995/kafka-connect-registry-validation-smt/releases/download/v0.3.0/kafka-connect-registry-validation-smt-0.3.0.jar \
    /usr/share/confluent-hub-components/confluentinc-kafka-connect-json-schema-converter/lib/
```

Built and tested against Confluent Platform 7.9.2 (Apache Kafka 3.9), Java 17.

## Configuration

| Property | Default | Description |
|---|---|---|
| `schema.registry.url` | — | Schema Registry URL (required) |
| `basic.auth.user.info` | — | Schema Registry credentials `key:secret` |
| `subject` | `<topic>-value` | Subject to validate against |
| `raw.field` | `message` | Field holding the raw line, removed from valid records. Empty string to disable |
| `dlq.topic` | — | Dead letter topic. Overrides `dlq.topic.suffix` when set |
| `dlq.topic.suffix` | `.dlq` | Suffix appended to the record topic when `dlq.topic` is not set |
| `dlq.value.mode` | `envelope` | `envelope` or `original` (see above) |
| `dlq.header.prefix` | `dlq.` | Prefix of the error headers |
| `source.position.headers` | `false` | Add the source partition and offset as JSON headers on DLQ records |
| `source` | — | Value of the `source` field (envelope mode) |
| `file.name.header` | `connect.file.name` | Header copied into `fileName` (envelope mode) |
| `line.number.header` | `connect.file.line.number` | Header copied into `lineNumber` (envelope mode) |
| `schema.cache.ttl.ms` | `300000` | Schema cache duration |

Place the SMT **last** in the transform chain, after any SMT changing the topic, since the topic selects the subject.

### Example with FilePulse (original mode)

FilePulse adds the origin (`connect.file.name` automatically, the line number with a filter), routes invalid JSON
itself, and writes its error to the same header as the SMT:

```json
"filters": "AddLineNumberHeader, ParseJSON",
"filters.AddLineNumberHeader.type": "io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter",
"filters.AddLineNumberHeader.field": "$headers.connect.file.line.number",
"filters.AddLineNumberHeader.value": "{{ $offset.rows }}",
"filters.ParseJSON.type": "io.streamthoughts.kafka.connect.filepulse.filter.JSONFilter",
"filters.ParseJSON.source": "message",
"filters.ParseJSON.merge": "true",
"filters.ParseJSON.withOnFailure": "ToErrorTopic, AddErrorHeader",
"filters.ToErrorTopic.type": "io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter",
"filters.ToErrorTopic.field": "$topic",
"filters.ToErrorTopic.value": "dev.orders.rejected",
"filters.AddErrorHeader.type": "io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter",
"filters.AddErrorHeader.field": "$headers.dlq.error.message",
"filters.AddErrorHeader.value": "{{ $error.exceptionMessage }}",

"transforms": "validate",
"transforms.validate.type": "com.pluxee.connect.transforms.ValidateAgainstRegistry",
"transforms.validate.schema.registry.url": "https://schema-registry:8081",
"transforms.validate.basic.auth.user.info": "${file:/mnt/secrets/sr.properties:basic.auth.user.info}",
"transforms.validate.dlq.topic": "dev.orders.rejected",
"transforms.validate.dlq.value.mode": "original",

"value.converter": "io.confluent.connect.json.JsonSchemaConverter",
"value.converter.auto.register.schemas": "false",
"value.converter.use.latest.version": "true",
"value.converter.json.fail.invalid.schema": "true"
```

Use the same value for `filters.ToErrorTopic.value` and `transforms.validate.dlq.topic`, so both paths land on one
dead letter topic and the SMT skips the records FilePulse already rerouted. Keep the raw field (do **not** exclude
`message` in FilePulse): the SMT removes it from valid records.

### Example with JDBC

```json
"transforms": "validate",
"transforms.validate.type": "com.pluxee.connect.transforms.ValidateAgainstRegistry",
"transforms.validate.schema.registry.url": "https://schema-registry:8081",
"transforms.validate.raw.field": "",
"transforms.validate.dlq.value.mode": "original",
"transforms.validate.source.position.headers": "true"
```

The source position headers identify the table and the incremental offset of each rejected row.

## Build

```shell
mvn verify
```

## Release

Push a tag `vX.Y.Z`: the release workflow sets the version from the tag, builds, tests and attaches the jar to a
GitHub release.

```shell
git tag v0.3.0
git push origin v0.3.0
```

## License

Apache License 2.0, see [LICENSE](LICENSE).
