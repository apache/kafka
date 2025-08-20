/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MaskFieldTest {

    private static final Schema SCHEMA = SchemaBuilder.struct()
        .field("magic", Schema.INT32_SCHEMA)
        .field("bool", Schema.BOOLEAN_SCHEMA)
        .field("byte", Schema.INT8_SCHEMA)
        .field("short", Schema.INT16_SCHEMA)
        .field("int", Schema.INT32_SCHEMA)
        .field("long", Schema.INT64_SCHEMA)
        .field("float", Schema.FLOAT32_SCHEMA)
        .field("double", Schema.FLOAT64_SCHEMA)
        .field("string", Schema.STRING_SCHEMA)
        .field("date", org.apache.kafka.connect.data.Date.SCHEMA)
        .field("time", Time.SCHEMA)
        .field("timestamp", Timestamp.SCHEMA)
        .field("decimal", Decimal.schema(0))
        .field("array", SchemaBuilder.array(Schema.INT32_SCHEMA))
        .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA))
        .field("withDefault", SchemaBuilder.string().optional().defaultValue("default").build())
        .build();

    private static final Map<String, Object> VALUES = Map.ofEntries(
        Map.entry("magic", 42),
        Map.entry("bool", true),
        Map.entry("byte", (byte) 42),
        Map.entry("short", (short) 42),
        Map.entry("int", 42),
        Map.entry("long", 42L),
        Map.entry("float", 42f),
        Map.entry("double", 42d),
        Map.entry("string", "55.121.20.20"),
        Map.entry("date", new Date()),
        Map.entry("bigint", new BigInteger("42")),
        Map.entry("bigdec", new BigDecimal("42.0")),
        Map.entry("list", List.of(42)),
        Map.entry("map", Map.of("key", "value"))
    );

    private static final Struct VALUES_WITH_SCHEMA = new Struct(SCHEMA)
        .put("magic", 42)
        .put("bool", true)
        .put("byte", (byte) 42)
        .put("short", (short) 42)
        .put("int", 42)
        .put("long", 42L)
        .put("float", 42f)
        .put("double", 42d)
        .put("string", "hmm")
        .put("date", new Date())
        .put("time", new Date())
        .put("timestamp", new Date())
        .put("decimal", new BigDecimal(42))
        .put("array", List.of(1, 2, 3))
        .put("map", Map.of("what", "what"))
        .put("withDefault", null);

    private static MaskField<SinkRecord> transform(List<String> fields, String replacement) {
        final MaskField<SinkRecord> xform = new MaskField.Value<>();
        Map<String, Object> props = new HashMap<>();
        props.put("fields", fields);
        props.put("replacement", replacement);
        props.put("replace.null.with.default", false);
        xform.configure(props);
        return xform;
    }

    private static SinkRecord record(Schema schema, Object value) {
        return new SinkRecord("", 0, null, null, schema, value, 0);
    }

    private static void checkReplacementWithSchema(String maskField, Object replacement) {
        SinkRecord record = record(SCHEMA, VALUES_WITH_SCHEMA);
        final Struct updatedValue = (Struct) transform(List.of(maskField), String.valueOf(replacement)).apply(record).value();
        assertEquals(replacement, updatedValue.get(maskField), "Invalid replacement for " + maskField + " value");
    }

    private static void checkReplacementSchemaless(String maskField, Object replacement) {
        checkReplacementSchemaless(List.of(maskField), replacement);
    }

    @SuppressWarnings("unchecked")
    private static void checkReplacementSchemaless(List<String> maskFields, Object replacement) {
        SinkRecord record = record(null, VALUES);
        final Map<String, Object> updatedValue = (Map) transform(maskFields, String.valueOf(replacement))
            .apply(record)
            .value();
        for (String maskField : maskFields) {
            assertEquals(replacement, updatedValue.get(maskField), "Invalid replacement for " + maskField + " value");
        }
    }

    @Test
    public void testSchemaless() {
        final List<String> maskFields = new ArrayList<>(VALUES.keySet());
        maskFields.remove("magic");
        @SuppressWarnings("unchecked") final Map<String, Object> updatedValue = (Map) transform(maskFields, null).apply(record(null, VALUES)).value();

        assertEquals(42, updatedValue.get("magic"));
        assertEquals(false, updatedValue.get("bool"));
        assertEquals((byte) 0, updatedValue.get("byte"));
        assertEquals((short) 0, updatedValue.get("short"));
        assertEquals(0, updatedValue.get("int"));
        assertEquals(0L, updatedValue.get("long"));
        assertEquals(0f, updatedValue.get("float"));
        assertEquals(0d, updatedValue.get("double"));
        assertEquals("", updatedValue.get("string"));
        assertEquals(new Date(0), updatedValue.get("date"));
        assertEquals(BigInteger.ZERO, updatedValue.get("bigint"));
        assertEquals(BigDecimal.ZERO, updatedValue.get("bigdec"));
        assertEquals(List.of(), updatedValue.get("list"));
        assertEquals(Map.of(), updatedValue.get("map"));
    }

    @Test
    public void testWithSchema() {
        final List<String> maskFields = new ArrayList<>(SCHEMA.fields().size());
        for (Field field : SCHEMA.fields()) {
            if (!field.name().equals("magic")) {
                maskFields.add(field.name());
            }
        }

        final Struct updatedValue = (Struct) transform(maskFields, null).apply(record(SCHEMA, VALUES_WITH_SCHEMA)).value();

        assertEquals(42, updatedValue.get("magic"));
        assertEquals(false, updatedValue.get("bool"));
        assertEquals((byte) 0, updatedValue.get("byte"));
        assertEquals((short) 0, updatedValue.get("short"));
        assertEquals(0, updatedValue.get("int"));
        assertEquals(0L, updatedValue.get("long"));
        assertEquals(0f, updatedValue.get("float"));
        assertEquals(0d, updatedValue.get("double"));
        assertEquals("", updatedValue.get("string"));
        assertEquals(new Date(0), updatedValue.get("date"));
        assertEquals(new Date(0), updatedValue.get("time"));
        assertEquals(new Date(0), updatedValue.get("timestamp"));
        assertEquals(BigDecimal.ZERO, updatedValue.get("decimal"));
        assertEquals(List.of(), updatedValue.get("array"));
        assertEquals(Map.of(), updatedValue.get("map"));
        assertEquals(null, updatedValue.getWithoutDefault("withDefault"));
    }

    @Test
    public void testSchemalessWithReplacement() {
        checkReplacementSchemaless("short", (short) 123);
        checkReplacementSchemaless("byte", (byte) 123);
        checkReplacementSchemaless("int", 123);
        checkReplacementSchemaless("long", 123L);
        checkReplacementSchemaless("float", 123.0f);
        checkReplacementSchemaless("double", 123.0);
        checkReplacementSchemaless("string", "123");
        checkReplacementSchemaless("bigint", BigInteger.valueOf(123L));
        checkReplacementSchemaless("bigdec", BigDecimal.valueOf(123.0));
    }

    @Test
    public void testSchemalessUnsupportedReplacementType() {
        String exMessage = "Cannot mask value of type";
        Class<DataException> exClass = DataException.class;

        assertThrows(exClass, () -> checkReplacementSchemaless("date", new Date()), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless(List.of("int", "date"), new Date()), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("bool", false), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("list", List.of("123")), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("map", Map.of("123", "321")), exMessage);
    }

    @Test
    public void testWithSchemaAndReplacement() {
        checkReplacementWithSchema("short", (short) 123);
        checkReplacementWithSchema("byte", (byte) 123);
        checkReplacementWithSchema("int", 123);
        checkReplacementWithSchema("long", 123L);
        checkReplacementWithSchema("float", 123.0f);
        checkReplacementWithSchema("double", 123.0);
        checkReplacementWithSchema("string", "123");
        checkReplacementWithSchema("decimal", BigDecimal.valueOf(123.0));
    }

    @Test
    public void testWithSchemaUnsupportedReplacementType() {
        String exMessage = "Cannot mask value of type";
        Class<DataException> exClass = DataException.class;

        assertThrows(exClass, () -> checkReplacementWithSchema("time", new Date()), exMessage);
        assertThrows(exClass, () -> checkReplacementWithSchema("timestamp", new Date()), exMessage);
        assertThrows(exClass, () -> checkReplacementWithSchema("array", List.of(123)), exMessage);
    }

    @Test
    public void testReplacementTypeMismatch() {
        String exMessage = "Invalid value  for configuration replacement";
        Class<DataException> exClass = DataException.class;

        assertThrows(exClass, () -> checkReplacementSchemaless("byte", "foo"), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("short", "foo"), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("int", "foo"), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("long", "foo"), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("float", "foo"), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("double", "foo"), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("bigint", "foo"), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("bigdec", "foo"), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("int", new Date()), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("int", new Object()), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless(List.of("string", "int"), "foo"), exMessage);
    }

    @Test
    public void testEmptyStringReplacementValue() {
        assertThrows(ConfigException.class, () -> checkReplacementSchemaless("short", ""), "String must be non-empty");
    }

    @Test
    public void testNullListAndMapReplacementsAreMutable() {
        final List<String> maskFields = List.of("array", "map");
        final Struct updatedValue = (Struct) transform(maskFields, null).apply(record(SCHEMA, VALUES_WITH_SCHEMA)).value();
        @SuppressWarnings("unchecked") List<Integer> actualList = (List<Integer>) updatedValue.get("array");
        assertEquals(List.of(), actualList);
        actualList.add(0);
        assertEquals(List.of(0), actualList);

        @SuppressWarnings("unchecked") Map<String, String> actualMap = (Map<String, String>) updatedValue.get("map");
        assertEquals(Map.of(), actualMap);
        actualMap.put("k", "v");
        assertEquals(Map.of("k", "v"), actualMap);
    }

    @Test
    public void testMaskFieldReturnsVersionFromAppInfoParser() {
        final MaskField<SinkRecord> xform = new MaskField.Value<>();
        assertEquals(AppInfoParser.getVersion(), xform.version());
    }
}
