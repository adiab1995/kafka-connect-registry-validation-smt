package com.pluxee.connect.transforms;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
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

        transform = new ValidateAgainstRegistry<>(registry);
        transform.configure(Map.of(
            ValidateAgainstRegistry.SCHEMA_REGISTRY_URL, "mock://unused",
            ValidateAgainstRegistry.SOURCE, "order"));
    }

    @AfterEach
    void tearDown() {
        transform.close();
    }

    @Test
    void validRecordKeepsTopicAndDropsRawField() {
        SourceRecord result = transform.apply(record(TOPIC, order("A1", "02", List.of(item("SKU-1", "1.50")), ITEM)));

        assertEquals(TOPIC, result.topic());
        Struct value = (Struct) result.value();
        assertNull(value.schema().field("message"));
        assertEquals("A1", value.getString("orderId"));
    }

    @Test
    void missingRequiredFieldGoesToDlq() {
        Schema schema = SchemaBuilder.struct()
            .field("message", Schema.STRING_SCHEMA)
            .field("productCode", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
        Struct value = new Struct(schema).put("message", RAW).put("productCode", "02");

        Struct dlq = assertDlq(transform.apply(record(TOPIC, value)));

        assertTrue(dlq.getString("error").contains("orderId"), dlq.getString("error"));
    }

    @Test
    void patternViolationGoesToDlq() {
        Struct dlq = assertDlq(transform.apply(record(TOPIC, order("A1", "002", null, ITEM))));

        assertTrue(dlq.getString("error").contains("productCode"), dlq.getString("error"));
    }

    @Test
    void nestedTypeViolationGoesToDlq() {
        Struct item = new Struct(ITEM_NUMERIC_AMOUNT).put("sku", "SKU-1").put("amount", 1.5);

        Struct dlq = assertDlq(transform.apply(record(TOPIC, order("A1", "02", List.of(item), ITEM_NUMERIC_AMOUNT))));

        assertTrue(dlq.getString("error").contains("items/0/amount"), dlq.getString("error"));
    }

    @Test
    void nestedRequiredViolationGoesToDlq() {
        Struct dlq = assertDlq(transform.apply(record(TOPIC, order("A1", "02", List.of(item(null, "1.50")), ITEM))));

        assertTrue(dlq.getString("error").contains("items/0"), dlq.getString("error"));
    }

    @Test
    void dlqRecordCarriesRawLineAndHeaders() {
        Struct dlq = assertDlq(transform.apply(record(TOPIC, order("A1", "002", null, ITEM))));

        assertEquals(RAW, dlq.getString("message"));
        assertEquals("order", dlq.getString("source"));
        assertEquals("orders.zip", dlq.getString("fileName"));
        assertEquals(42, dlq.getInt32("lineNumber"));
    }

    @Test
    void recordsAlreadyOnDlqTopicAreUntouched() {
        SourceRecord record = record(TOPIC + ".dlq", order("A1", "002", null, ITEM));

        assertSame(record, transform.apply(record));
    }

    @Test
    void explicitDlqTopicReceivesInvalidRecords() {
        ValidateAgainstRegistry<SourceRecord> explicit = withDlqTopic("dev.errors");

        SourceRecord result = explicit.apply(record(TOPIC, order("A1", "002", null, ITEM)));

        assertEquals("dev.errors", result.topic());
        assertEquals(ValidateAgainstRegistry.DLQ_SCHEMA, result.valueSchema());
        assertEquals(TOPIC, explicit.apply(record(TOPIC, order("A1", "02", null, ITEM))).topic());
    }

    @Test
    void recordsAlreadyOnExplicitDlqTopicAreUntouched() {
        ValidateAgainstRegistry<SourceRecord> explicit = withDlqTopic("dev.errors");
        SourceRecord record = record("dev.errors", order("A1", "002", null, ITEM));

        assertSame(record, explicit.apply(record));
    }

    @Test
    void registryErrorFailsTheRecordInsteadOfRoutingToDlq() {
        SourceRecord record = record("unknown.topic", order("A1", "02", null, ITEM));

        assertThrows(ConnectException.class, () -> transform.apply(record));
    }

    private ValidateAgainstRegistry<SourceRecord> withDlqTopic(String dlqTopic) {
        ValidateAgainstRegistry<SourceRecord> explicit = new ValidateAgainstRegistry<>(registry);
        explicit.configure(Map.of(
            ValidateAgainstRegistry.SCHEMA_REGISTRY_URL, "mock://unused",
            ValidateAgainstRegistry.DLQ_TOPIC, dlqTopic));
        return explicit;
    }

    private static Struct assertDlq(SourceRecord result) {
        assertEquals(TOPIC + ".dlq", result.topic());
        assertEquals(ValidateAgainstRegistry.DLQ_SCHEMA, result.valueSchema());
        return (Struct) result.value();
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
        return new SourceRecord(Map.of(), Map.of(), topic, null, null, null, value.schema(), value, null, headers);
    }
}
