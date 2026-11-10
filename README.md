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
- Schema Registry errors fail the record instead of sending it to the dead letter topic.
- Schemas are cached and refreshed every `schema.cache.ttl.ms`.

Since invalid records do not match the business schema, the subject of the dead letter topic must accept any object,
e.g. `{"type": "object"}`.

## Installation

The SMT relies on the classes of the Confluent JSON Schema converter. Copy the jar into the **lib directory of the
`kafka-connect-json-schema-converter` plugin**, so both share the same plugin class loader:

```dockerfile
ADD https://github.com/adiab1995/kafka-connect-registry-validation-smt/releases/download/v1.0.0/kafka-connect-registry-validation-smt-1.0.0.jar \
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
| `schema.cache.ttl.ms` | `300000` | Schema cache duration |

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

"value.converter": "io.confluent.connect.json.JsonSchemaConverter",
"value.converter.auto.register.schemas": "false",
"value.converter.use.latest.version": "true",
"value.converter.json.fail.invalid.schema": "true"
```

Use the same value for `filters.ToErrorTopic.value` and `transforms.validate.dlq.topic`, so both paths land on one
dead letter topic and the SMT skips the records FilePulse already rerouted.

## Build

```shell
mvn verify
```

## Release

Push a tag `vX.Y.Z`: the release workflow sets the version from the tag, builds, tests and attaches the jar to a
GitHub release.

```shell
git tag v1.0.0
git push origin v1.0.0
```

## License

Apache License 2.0, see [LICENSE](LICENSE).
