# Kafka Connect Registry Validation SMT

A Kafka Connect Single Message Transform that validates each record value against the **JSON Schema registered in
Confluent Schema Registry** and reroutes invalid records to a dead letter topic, instead of letting the converter drop
them.

Source connectors have no dead letter queue: with `errors.tolerance=all`, a record rejected by `JsonSchemaConverter`
(`json.fail.invalid.schema=true`) is only logged and lost. This SMT runs the same validation one step earlier, while
the record can still be rerouted.

## How it works

For every record whose value is a `Struct`:

1. Removes the raw field (default `message`) from the value.
2. Converts the value to JSON with `JsonSchemaData`, exactly as `JsonSchemaConverter` does.
3. Validates it against the latest schema of `<topic>-value` (or the configured `subject`).
4. **Valid** → forwarded on the same topic, without the raw field.
   **Invalid** → sent to `<topic>.dlq` with this value:

```json
{
  "message": "<raw line>",
  "error": "#/items/0/amount: expected type: string, actual: number",
  "source": "orders",
  "fileName": "orders_2026.zip",
  "lineNumber": 5
}
```

All validation rules come from the registered schema: required fields, nested objects, types, patterns, `minItems`…
No field list has to be maintained in the connector configuration. Schemas are cached and refreshed every
`schema.cache.ttl.ms`.

Records already on a topic ending with the DLQ suffix are left untouched. Schema Registry errors fail the record
(they are not routed to the DLQ).

## Installation

The SMT relies on the classes of the Confluent JSON Schema converter. Copy the jar into the **lib directory of the
`kafka-connect-json-schema-converter` plugin**, so both share the same plugin class loader:

```dockerfile
ADD https://github.com/adiab1995/kafka-connect-registry-validation-smt/releases/download/v0.1.0/kafka-connect-registry-validation-smt-0.1.0.jar \
    /usr/share/confluent-hub-components/confluentinc-kafka-connect-json-schema-converter/lib/
```

Built and tested against Confluent Platform 7.9.2 (Apache Kafka 3.9), Java 17.

## Configuration

| Property | Default | Description |
|---|---|---|
| `schema.registry.url` | — | Schema Registry URL (required) |
| `basic.auth.user.info` | — | Schema Registry credentials `key:secret` |
| `subject` | `<topic>-value` | Subject to validate against |
| `raw.field` | `message` | Field holding the raw line: removed from valid records, kept in DLQ records |
| `dlq.topic.suffix` | `.dlq` | Suffix of the dead letter topic |
| `source` | — | Value of the `source` field in DLQ records |
| `file.name.header` | `connect.file.name` | Header holding the source file name |
| `line.number.header` | `connect.file.line.number` | Header holding the source line number |
| `schema.cache.ttl.ms` | `300000` | Schema cache duration |

### Example with FilePulse

```json
"filters": "AddLineNumberHeader, ParseJSON",
"filters.AddLineNumberHeader.type": "io.streamthoughts.kafka.connect.filepulse.filter.AppendFilter",
"filters.AddLineNumberHeader.field": "$headers.connect.file.line.number",
"filters.AddLineNumberHeader.value": "{{ $offset.rows }}",
"filters.ParseJSON.type": "io.streamthoughts.kafka.connect.filepulse.filter.JSONFilter",
"filters.ParseJSON.source": "message",
"filters.ParseJSON.merge": "true",

"transforms": "validate",
"transforms.validate.type": "com.pluxee.connect.transforms.ValidateAgainstRegistry",
"transforms.validate.schema.registry.url": "https://schema-registry:8081",
"transforms.validate.basic.auth.user.info": "${file:/mnt/secrets/sr.properties:basic.auth.user.info}",
"transforms.validate.source": "orders",

"value.converter": "io.confluent.connect.json.JsonSchemaConverter",
"value.converter.auto.register.schemas": "false",
"value.converter.use.latest.version": "true",
"value.converter.json.fail.invalid.schema": "true"
```

Keep the raw field (do **not** exclude `message` in FilePulse): the SMT removes it from valid records and keeps it in
DLQ records. The DLQ topic needs a registered schema matching the DLQ value above.

## Build

```shell
mvn verify
```

## Release

Push a tag `vX.Y.Z`: the release workflow sets the version from the tag, builds, tests and attaches the jar to a
GitHub release.

```shell
git tag v0.1.0
git push origin v0.1.0
```

## License

Apache License 2.0, see [LICENSE](LICENSE).
