# Kafka Connect Registry Validation SMT

A Kafka Connect Single Message Transform that validates each record value against the **JSON Schema registered in
Confluent Schema Registry**, and sends invalid records to a dead letter topic instead of letting the converter drop
them.

Source connectors have no dead letter queue: with `errors.tolerance=all`, a record rejected by `JsonSchemaConverter`
(`json.fail.invalid.schema=true`) is only logged and lost. This SMT runs the same validation one step earlier, while
the record can still be rerouted.

## How it works

For every record whose value is a `Struct`, the SMT converts the value to JSON exactly as `JsonSchemaConverter` does,
and validates it against the latest schema of `<topic>-value` (or the configured `subject`):

- **Valid** → the record is forwarded unchanged.
- **Invalid** → the record is forwarded unchanged too (same key, value, timestamp and headers), but to the dead letter
  topic (`dlq.topic`, or `<topic>.dlq` by default), with three extra headers:

| Header | Example |
|---|---|
| `dlq.error.message` | `#/items/0/amount: expected type: string, actual: number` |
| `dlq.error.subject` | `dev.orders-value` |
| `dlq.original.topic` | `dev.orders` |

All validation rules come from the registered schema: required fields, nested objects, types, patterns, `minItems`…
Nothing about the data or the source connector is configured in the SMT: the origin of a record (file name, line
number, table…) is whatever headers the source connector puts on it.

Other behaviours:

- Records already on the dead letter topic are left untouched (the topic equal to `dlq.topic`, or any topic ending
  with `dlq.topic.suffix`), so an upstream step can write to the same dead letter topic.
- Values that are not a `Struct` (schemaless maps, strings) are forwarded unvalidated, with a warning logged once per
  topic. Tombstones are forwarded.
- Schemas are loaded with their **references**, so `$ref` to other subjects resolve. Only JSON Schema subjects are
  supported: an Avro or Protobuf subject fails the record with an explicit error.
- Schemas are cached and refreshed every `schema.cache.ttl.ms`. **If a refresh fails** (Schema Registry outage), the
  previous schema keeps being used. If no schema was ever loaded, the record fails with a retriable error: set
  `errors.retry.timeout` on the connector so Connect retries instead of dropping it. A subject that does not exist
  fails the record without retry.
- Error messages carry the path of the invalid field. They are computed without any network access.

### The rerouted record goes through the converter

An SMT runs **before** the converter: the rejected record is serialized by the connector's `value.converter` like any
other record. Two consequences, both silent with `errors.tolerance=all`:

- **The dead letter subject must exist and accept rejected records**, e.g. `{"type": "object"}`, otherwise rejected
  records are dropped by the converter. When `dlq.topic` is set, the SMT checks that `<dlq.topic>-value` exists at
  startup and **fails the connector** otherwise (`dlq.subject.check`).
- **Set `value.converter.latest.compatibility.strict=false`**. With the default `true`, the converter also checks that
  the latest registered schema is backward compatible with the schema Connect generates, and can drop a record the SMT
  accepted.

The SMT converts records to JSON like the converter: pass it the same conversion settings with the `json.data.`
prefix (e.g. `value.converter.decimal.format=NUMERIC` → `transforms.validate.json.data.decimal.format=NUMERIC`).

## Installation

The SMT relies on the classes of the Confluent JSON Schema converter. Copy the jar into the **lib directory of the
`kafka-connect-json-schema-converter` plugin**, so both share the same plugin class loader:

```dockerfile
ADD https://github.com/adiab1995/kafka-connect-registry-validation-smt/releases/download/v1.1.0/kafka-connect-registry-validation-smt-1.1.0.jar \
    /usr/share/confluent-hub-components/confluentinc-kafka-connect-json-schema-converter/lib/
```

Built and tested against Confluent Platform 7.9.2 (Apache Kafka 3.9), Java 17.

## Configuration

| Property | Default | Description |
|---|---|---|
| `schema.registry.url` | — | Schema Registry URL (required) |
| `basic.auth.user.info` | — | Schema Registry credentials `key:secret` |
| `subject` | `<topic>-value` | Subject to validate against |
| `dlq.topic` | — | Dead letter topic. Overrides `dlq.topic.suffix` when set |
| `dlq.topic.suffix` | `.dlq` | Suffix appended to the record topic when `dlq.topic` is not set |
| `dlq.subject.check` | `true` | With `dlq.topic`, fail at startup if `<dlq.topic>-value` does not exist |
| `strip.fields` | — | Top-level fields removed from valid records before validation and production (e.g. a raw `message` field). Records sent to the dead letter topic keep them |
| `schema.cache.ttl.ms` | `300000` | Schema cache duration. The cached schema is kept if a refresh fails |
| `json.data.*` | — | Connect to JSON conversion settings, same as the converter's (e.g. `json.data.decimal.format`) |

Place the SMT **last** in the transform chain, after any SMT changing the topic, since the topic selects the subject.

### Example with FilePulse

FilePulse adds the origin headers (`connect.file.name` natively, the line number with an `AppendFilter`), and routes
invalid JSON lines itself to the same dead letter topic with the same error header:

```json
"filters": "AddLineNumberHeader, ParseJSON",
"filters.AddLineNumberHeader.type": "io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter",
"filters.AddLineNumberHeader.field": "$headers.connect.file.line.number",
"filters.AddLineNumberHeader.value": "{{ $offset.rows }}",
"filters.ParseJSON.type": "io.streamthoughts.kafka.connect.filepulse.filter.JSONFilter",
"filters.ParseJSON.source": "message",
"filters.ParseJSON.merge": "true",
"filters.ParseJSON.withOnFailure": "ToErrorTopic, AddErrorHeader, AddOriginalTopicHeader",
"filters.ToErrorTopic.type": "io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter",
"filters.ToErrorTopic.field": "$topic",
"filters.ToErrorTopic.value": "dev.orders.rejected",
"filters.AddErrorHeader.type": "io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter",
"filters.AddErrorHeader.field": "$headers.dlq.error.message",
"filters.AddErrorHeader.value": "{{ $error.exceptionMessage }}",
"filters.AddOriginalTopicHeader.type": "io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter",
"filters.AddOriginalTopicHeader.field": "$headers.dlq.original.topic",
"filters.AddOriginalTopicHeader.value": "dev.orders",

"transforms": "validate",
"transforms.validate.type": "com.pluxee.connect.transforms.ValidateAgainstRegistry",
"transforms.validate.schema.registry.url": "https://schema-registry:8081",
"transforms.validate.basic.auth.user.info": "${file:/mnt/secrets/sr.properties:basic.auth.user.info}",
"transforms.validate.dlq.topic": "dev.orders.rejected",
"transforms.validate.strip.fields": "message",

"value.converter": "io.confluent.connect.json.JsonSchemaConverter",
"value.converter.auto.register.schemas": "false",
"value.converter.use.latest.version": "true",
"value.converter.latest.compatibility.strict": "false",
"value.converter.json.fail.invalid.schema": "true",

"errors.tolerance": "all",
"errors.retry.timeout": "300000"
```

Use the same value for `filters.ToErrorTopic.value` and `transforms.validate.dlq.topic`, so both paths land on one
dead letter topic and the SMT skips the records FilePulse already rerouted.

`JSONFilter` with `merge=true` keeps the raw `message` field in the record. `strip.fields=message` removes it from valid
records (required if the business schema has `additionalProperties: false`), while rejected records keep it.

## Build

```shell
mvn verify
```

## Release

Push a tag `vX.Y.Z`: the release workflow sets the version from the tag, builds, tests and attaches the jar to a
GitHub release.

```shell
git tag v1.1.0
git push origin v1.1.0
```

## License

Apache License 2.0, see [LICENSE](LICENSE).
