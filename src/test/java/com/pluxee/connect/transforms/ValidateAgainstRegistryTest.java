package com.pluxee.connect.transforms;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Runs with network access blocked (see the surefire configuration): nothing may depend on it. */
class ValidateAgainstRegistryTest {

    private static final String TOPIC = "dev.orders";
    private static final String DLQ = TOPIC + ".dlq";
    private static final String RAW = "{\"raw\":\"line\"}";
    private static final String OPEN_SCHEMA = "{\"type\":\"object\"}";

    private static final Schema ITEM = SchemaBuilder.struct().optional()
        .field("sku", Schema.OPTIONAL_STRING_SCHEMA)
        .field("amount", Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    private static final Schema ITEM_NUMERIC_AMOUNT = SchemaBuilder.struct().optional()
        .field("sku", Schema.OPTIONAL_STRING_SCHEMA)
        .field("amount", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .build();

    private FlakyRegistry registry;
    private ValidateAgainstRegistry<SourceRecord> transform;

    /** Mock registry whose lookups can be made to fail, to simulate outages and server errors. */
    static class FlakyRegistry extends MockSchemaRegistryClient {
        boolean down;
        RestClientException serverError;

        @Override
        public SchemaMetadata getLatestSchemaMetadata(String subject) throws IOException, RestClientException {
            if (down) {
                throw new IOException("Schema Registry unreachable");
            }
            if (serverError != null) {
                throw serverError;
            }
            return super.getLatestSchemaMetadata(subject);
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        registry = new FlakyRegistry();
        registry.register(TOPIC + "-value", new JsonSchema(resource("/order-schema.json")));
        registry.register("dev.errors-value", new JsonSchema(OPEN_SCHEMA));
        transform = configured(Map.of());
    }

    // --- validation and rerouting

    @Test
    void validRecordIsForwardedUnchanged() {
        SourceRecord record = record(TOPIC, order("A1", "02", List.of(item("SKU-1", "1.50")), ITEM));

        assertSame(record, transform.apply(record));
    }

    @Test
    void invalidRecordIsForwardedUnchangedToDlqWithErrorHeaders() {
        SourceRecord record = record(TOPIC, order("A1", "002", null, ITEM));

        SourceRecord result = transform.apply(record);

        assertEquals(DLQ, result.topic());
        assertSame(record.key(), result.key());
        assertSame(record.value(), result.value());
        assertEquals(record.valueSchema(), result.valueSchema());
        assertEquals(record.timestamp(), result.timestamp());
        assertEquals("42", header(result, "connect.file.line.number"));
        assertTrue(header(result, ValidateAgainstRegistry.ERROR_MESSAGE_HEADER).contains("#/productCode"));
        assertEquals(TOPIC + "-value", header(result, ValidateAgainstRegistry.ERROR_SUBJECT_HEADER));
        assertEquals(TOPIC, header(result, ValidateAgainstRegistry.ORIGINAL_TOPIC_HEADER));
    }

    @Test
    void missingRequiredField() {
        Schema schema = SchemaBuilder.struct().field("productCode", Schema.OPTIONAL_STRING_SCHEMA).build();

        String error = dlqError(transform.apply(record(TOPIC, new Struct(schema).put("productCode", "02"))));

        assertTrue(error.contains("orderId"), error);
    }

    @Test
    void nestedTypeViolationIsDescribedWithItsPathWithoutNetwork() {
        Struct item = new Struct(ITEM_NUMERIC_AMOUNT).put("sku", "SKU-1").put("amount", 1.5);

        String error = dlqError(transform.apply(record(TOPIC, order("A1", "02", List.of(item), ITEM_NUMERIC_AMOUNT))));

        assertTrue(error.contains("#/items/0/amount"), error);
    }

    @Test
    void nestedRequiredViolation() {
        String error = dlqError(transform.apply(record(TOPIC, order("A1", "02", List.of(item(null, "1.50")), ITEM))));

        assertTrue(error.contains("#/items/0"), error);
    }

    @Test
    void errorHeadersDoNotLeakIntoTheIncomingRecord() {
        SourceRecord record = record(TOPIC, order("A1", "002", null, ITEM));

        transform.apply(record);

        assertNull(record.headers().lastWithName(ValidateAgainstRegistry.ERROR_MESSAGE_HEADER));
    }

    @Test
    void explicitDlqTopic() {
        ValidateAgainstRegistry<SourceRecord> explicit = configured(Map.of(ValidateAgainstRegistry.DLQ_TOPIC, "dev.errors"));

        assertEquals("dev.errors", explicit.apply(record(TOPIC, order("A1", "002", null, ITEM))).topic());
    }

    @Test
    void recordsAlreadyOnDlqTopicAreUntouched() {
        SourceRecord onSuffix = record(DLQ, order("A1", "002", null, ITEM));
        assertSame(onSuffix, transform.apply(onSuffix));

        ValidateAgainstRegistry<SourceRecord> explicit = configured(Map.of(ValidateAgainstRegistry.DLQ_TOPIC, "dev.errors"));
        SourceRecord onExplicit = record("dev.errors", order("A1", "002", null, ITEM));
        assertSame(onExplicit, explicit.apply(onExplicit));
    }

    @Test
    void nonStructValuesAndTombstonesAreForwarded() {
        SourceRecord map = new SourceRecord(Map.of(), Map.of(), TOPIC, null, null, null, null, Map.of("orderId", "A1"));
        SourceRecord tombstone = new SourceRecord(Map.of(), Map.of(), TOPIC, null, null, null, null, null);

        assertSame(map, transform.apply(map));
        assertSame(tombstone, transform.apply(tombstone));
    }

    // --- dead letter subject check

    @Test
    void missingDlqSubjectFailsAtStartup() {
        ConfigException e = assertThrows(ConfigException.class,
            () -> configured(Map.of(ValidateAgainstRegistry.DLQ_TOPIC, "dev.missing")));

        assertTrue(e.getMessage().contains("dev.missing-value"), e.getMessage());
    }

    @Test
    void dlqSubjectCheckCanBeDisabled() {
        assertDoesNotThrow(() -> configured(Map.of(
            ValidateAgainstRegistry.DLQ_TOPIC, "dev.missing",
            ValidateAgainstRegistry.DLQ_SUBJECT_CHECK, "false")));
    }

    // --- registry outages

    @Test
    void registryOutageKeepsUsingTheCachedSchema() {
        // A negative TTL refreshes on every record, so the outage is hit on the next lookup
        ValidateAgainstRegistry<SourceRecord> noCache = configured(Map.of(ValidateAgainstRegistry.SCHEMA_CACHE_TTL_MS, "-1"));
        SourceRecord valid = record(TOPIC, order("A1", "02", null, ITEM));
        assertSame(valid, noCache.apply(valid));

        registry.down = true;

        assertSame(valid, noCache.apply(valid));
        assertEquals(DLQ, noCache.apply(record(TOPIC, order("A1", "002", null, ITEM))).topic());
    }

    @Test
    void registryOutageWithoutCachedSchemaIsRetriable() {
        registry.down = true;

        assertThrows(RetriableException.class, () -> transform.apply(record(TOPIC, order("A1", "02", null, ITEM))));
    }

    @Test
    void registryServerErrorWithoutCachedSchemaIsRetriable() {
        registry.serverError = new RestClientException("Unavailable", 503, 50301);

        assertThrows(RetriableException.class, () -> transform.apply(record(TOPIC, order("A1", "02", null, ITEM))));
    }

    @Test
    void permanentRegistryErrorIsNotRetriable() {
        registry.serverError = new RestClientException("Unprocessable", 422, 42201);

        ConnectException e = assertThrows(ConnectException.class,
            () -> transform.apply(record(TOPIC, order("A1", "02", null, ITEM))));

        assertFalse(e instanceof RetriableException);
    }

    @Test
    void productionRegistryClientParsesJsonSchemas() throws Exception {
        SchemaRegistryClient client = ValidateAgainstRegistry.newRegistryClient("http://localhost:9", null);

        Optional<ParsedSchema> parsed = client.parseSchema(JsonSchema.TYPE, OPEN_SCHEMA, List.of());

        assertTrue(parsed.isPresent(), "JSON Schema provider missing from the registry client");
        assertTrue(parsed.get() instanceof JsonSchema);
    }

    @Test
    void unknownSubjectFailsTheRecordWithoutRetry() {
        SourceRecord record = record("unknown.topic", order("A1", "02", null, ITEM));

        ConnectException e = assertThrows(ConnectException.class, () -> transform.apply(record));

        assertFalse(e instanceof RetriableException);
    }

    // --- schema references

    @Test
    void schemaReferencesAreResolved() throws Exception {
        registry.register("dev.common.item-value", new JsonSchema(
            "{\"type\":\"object\",\"properties\":{\"sku\":{\"type\":\"string\"}},\"required\":[\"sku\"]}"));
        String main = "{\"type\":\"object\",\"properties\":{\"item\":{\"$ref\":\"item.json\"}},\"required\":[\"item\"]}";
        registry.register("dev.withrefs-value", new JsonSchema(main,
            List.of(new SchemaReference("item.json", "dev.common.item-value", 1)),
            Map.of("item.json", "{\"type\":\"object\",\"properties\":{\"sku\":{\"type\":\"string\"}},\"required\":[\"sku\"]}"),
            null));
        Schema itemSchema = SchemaBuilder.struct().field("sku", Schema.OPTIONAL_STRING_SCHEMA).build();
        Schema schema = SchemaBuilder.struct().field("item", itemSchema).build();

        SourceRecord valid = record("dev.withrefs", new Struct(schema).put("item", new Struct(itemSchema).put("sku", "S1")));
        SourceRecord invalid = record("dev.withrefs", new Struct(schema).put("item", new Struct(itemSchema)));

        assertSame(valid, transform.apply(valid));
        assertEquals("dev.withrefs.dlq", transform.apply(invalid).topic());
    }

    // --- strip.fields

    @Test
    void strippedFieldsAreRemovedFromValidRecordsAndKeptInDlqRecords() throws Exception {
        registry.register("dev.strict-value", new JsonSchema(
            "{\"type\":\"object\",\"additionalProperties\":false,"
            + "\"properties\":{\"orderId\":{\"type\":\"string\"},\"productCode\":{\"type\":\"string\",\"pattern\":\"^[0-9]{2}$\"},"
            + "\"items\":{\"type\":[\"null\",\"array\"]}}}"));
        ValidateAgainstRegistry<SourceRecord> strip = configured(Map.of(ValidateAgainstRegistry.STRIP_FIELDS, "message"));

        SourceRecord valid = strip.apply(record("dev.strict", order("A1", "02", null, ITEM)));
        SourceRecord invalid = strip.apply(record("dev.strict", order("A1", "002", null, ITEM)));

        assertEquals("dev.strict", valid.topic());
        assertNull(valid.valueSchema().field("message"));
        assertEquals("A1", ((Struct) valid.value()).getString("orderId"));
        assertEquals("dev.strict.dlq", invalid.topic());
        assertEquals(RAW, ((Struct) invalid.value()).getString("message"));
    }

    @Test
    void withoutStripFieldsAStrictSchemaRejectsTheRawField() throws Exception {
        registry.register("dev.strict-value", new JsonSchema(
            "{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"orderId\":{\"type\":\"string\"},"
            + "\"productCode\":{\"type\":\"string\"},\"items\":{\"type\":[\"null\",\"array\"]}}}"));

        assertEquals("dev.strict.dlq", transform.apply(record("dev.strict", order("A1", "02", null, ITEM))).topic());
    }

    // --- JSON conversion settings

    @Test
    void jsonDataSettingsMatchTheConverter() throws Exception {
        registry.register("dev.amounts-value", new JsonSchema(
            "{\"type\":\"object\",\"properties\":{\"amount\":{\"type\":\"number\"}}}"));
        Schema schema = SchemaBuilder.struct().field("amount", Decimal.builder(2).optional().build()).build();
        SourceRecord record = record("dev.amounts", new Struct(schema).put("amount", new BigDecimal("8.77")));

        ValidateAgainstRegistry<SourceRecord> numeric = configured(Map.of("json.data.decimal.format", "NUMERIC"));

        assertEquals("dev.amounts.dlq", transform.apply(record).topic());
        assertSame(record, numeric.apply(record));
    }

    // --- helpers

    private ValidateAgainstRegistry<SourceRecord> configured(Map<String, String> props) {
        Map<String, String> all = new HashMap<>(props);
        all.put(ValidateAgainstRegistry.SCHEMA_REGISTRY_URL, "mock://unused");
        ValidateAgainstRegistry<SourceRecord> configured = new ValidateAgainstRegistry<>(registry);
        configured.configure(all);
        return configured;
    }

    private String resource(String name) throws IOException {
        return new String(Objects.requireNonNull(getClass().getResourceAsStream(name)).readAllBytes(),
                          StandardCharsets.UTF_8);
    }

    private static String dlqError(SourceRecord result) {
        assertEquals(DLQ, result.topic());
        String error = header(result, ValidateAgainstRegistry.ERROR_MESSAGE_HEADER);
        assertNotNull(error);
        return error;
    }

    private static String header(SourceRecord record, String name) {
        Header header = record.headers().lastWithName(name);
        return header == null ? null : String.valueOf(header.value());
    }

    private static Struct item(String sku, String amount) {
        return new Struct(ITEM).put("sku", sku).put("amount", amount);
    }

    private static Struct order(String orderId, String productCode, List<Struct> items, Schema itemSchema) {
        Schema schema = SchemaBuilder.struct()
            .field("message", Schema.STRING_SCHEMA)
            .field("orderId", Schema.OPTIONAL_STRING_SCHEMA)
            .field("productCode", Schema.OPTIONAL_STRING_SCHEMA)
            .field("items", SchemaBuilder.array(itemSchema).optional().build())
            .build();
        return new Struct(schema)
            .put("message", RAW)
            .put("orderId", orderId)
            .put("productCode", productCode)
            .put("items", items);
    }

    private static SourceRecord record(String topic, Struct value) {
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("connect.file.name", "orders.zip");
        headers.addString("connect.file.line.number", "42");
        return new SourceRecord(Map.of(), Map.of(), topic, null, Schema.STRING_SCHEMA, "key-1",
                                value.schema(), value, 1_700_000_000_000L, headers);
    }
}
