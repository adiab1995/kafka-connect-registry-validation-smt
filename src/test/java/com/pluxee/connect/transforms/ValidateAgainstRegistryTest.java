package com.pluxee.connect.transforms;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValidateAgainstRegistryTest {

    private static final String TOPIC = "dev.orders";
    private static final String DLQ = TOPIC + ".dlq";
    private static final String RAW = "{\"raw\":\"line\"}";

    private static final Schema ITEM = SchemaBuilder.struct().optional()
        .field("sku", Schema.OPTIONAL_STRING_SCHEMA)
        .field("amount", Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    private static final Schema ITEM_NUMERIC_AMOUNT = SchemaBuilder.struct().optional()
        .field("sku", Schema.OPTIONAL_STRING_SCHEMA)
        .field("amount", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .build();

    private MockSchemaRegistryClient registry;
    private ValidateAgainstRegistry<SourceRecord> transform;

    @BeforeEach
    void setUp() throws Exception {
        registry = new MockSchemaRegistryClient();
        String schema = new String(
            Objects.requireNonNull(getClass().getResourceAsStream("/order-schema.json")).readAllBytes(),
            StandardCharsets.UTF_8);
        registry.register(TOPIC + "-value", new JsonSchema(schema));
        transform = configured(Map.of());
    }

    @AfterEach
    void tearDown() {
        transform.close();
    }

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
        assertTrue(header(result, ValidateAgainstRegistry.ERROR_MESSAGE_HEADER).contains("productCode"));
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
    void nestedTypeViolation() {
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

    @Test
    void registryErrorFailsTheRecordInsteadOfRoutingToDlq() {
        SourceRecord record = record("unknown.topic", order("A1", "02", null, ITEM));

        assertThrows(ConnectException.class, () -> transform.apply(record));
    }

    private ValidateAgainstRegistry<SourceRecord> configured(Map<String, String> props) {
        Map<String, String> all = new HashMap<>(props);
        all.put(ValidateAgainstRegistry.SCHEMA_REGISTRY_URL, "mock://unused");
        ValidateAgainstRegistry<SourceRecord> configured = new ValidateAgainstRegistry<>(registry);
        configured.configure(all);
        return configured;
    }

    private static String dlqError(SourceRecord result) {
        assertEquals(DLQ, result.topic());
        return header(result, ValidateAgainstRegistry.ERROR_MESSAGE_HEADER);
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
