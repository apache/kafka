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

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FlattenTest {
    private final Flatten<SourceRecord> xformKey = new Flatten.Key<>();
    private final Flatten<SourceRecord> xformValue = new Flatten.Value<>();

    @AfterEach
    public void teardown() {
        xformKey.close();
        xformValue.close();
    }

    @Test
    public void topLevelStructRequired() {
        xformValue.configure(Collections.emptyMap());
        assertThrows(DataException.class, () -> xformValue.apply(new SourceRecord(null, null,
                "topic", 0, Schema.INT32_SCHEMA, 42)));
    }

    @Test
    public void topLevelMapRequired() {
        xformValue.configure(Collections.emptyMap());
        assertThrows(DataException.class, () -> xformValue.apply(new SourceRecord(null, null,
                "topic", 0, null, 42)));
    }

    @Test
    public void testNestedStruct() {
        xformValue.configure(Collections.emptyMap());

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("int8", Schema.INT8_SCHEMA);
        builder.field("int16", Schema.INT16_SCHEMA);
        builder.field("int32", Schema.INT32_SCHEMA);
        builder.field("int64", Schema.INT64_SCHEMA);
        builder.field("float32", Schema.FLOAT32_SCHEMA);
        builder.field("float64", Schema.FLOAT64_SCHEMA);
        builder.field("boolean", Schema.BOOLEAN_SCHEMA);
        builder.field("string", Schema.STRING_SCHEMA);
        builder.field("bytes", Schema.BYTES_SCHEMA);
        Schema supportedTypesSchema = builder.build();

        builder = SchemaBuilder.struct();
        builder.field("B", supportedTypesSchema);
        Schema oneLevelNestedSchema = builder.build();

        builder = SchemaBuilder.struct();
        builder.field("A", oneLevelNestedSchema);
        Schema twoLevelNestedSchema = builder.build();

        Struct supportedTypes = new Struct(supportedTypesSchema);
        supportedTypes.put("int8", (byte) 8);
        supportedTypes.put("int16", (short) 16);
        supportedTypes.put("int32", 32);
        supportedTypes.put("int64", (long) 64);
        supportedTypes.put("float32", 32.f);
        supportedTypes.put("float64", 64.);
        supportedTypes.put("boolean", true);
        supportedTypes.put("string", "stringy");
        supportedTypes.put("bytes", "bytes".getBytes());

        Struct oneLevelNestedStruct = new Struct(oneLevelNestedSchema);
        oneLevelNestedStruct.put("B", supportedTypes);

        Struct twoLevelNestedStruct = new Struct(twoLevelNestedSchema);
        twoLevelNestedStruct.put("A", oneLevelNestedStruct);

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
                "topic", 0,
                twoLevelNestedSchema, twoLevelNestedStruct));

        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type());
        Struct transformedStruct = (Struct) transformed.value();
        assertEquals(9, transformedStruct.schema().fields().size());
        assertEquals(8, (byte) transformedStruct.getInt8("A.B.int8"));
        assertEquals(16, (short) transformedStruct.getInt16("A.B.int16"));
        assertEquals(32, (int) transformedStruct.getInt32("A.B.int32"));
        assertEquals(64L, (long) transformedStruct.getInt64("A.B.int64"));
        assertEquals(32.f, transformedStruct.getFloat32("A.B.float32"), 0.f);
        assertEquals(64., transformedStruct.getFloat64("A.B.float64"), 0.);
        assertEquals(true, transformedStruct.getBoolean("A.B.boolean"));
        assertEquals("stringy", transformedStruct.getString("A.B.string"));
        assertArrayEquals("bytes".getBytes(), transformedStruct.getBytes("A.B.bytes"));
    }

    @Test
    public void testNestedMapWithDelimiter() {
        xformValue.configure(Collections.singletonMap("delimiter", "#"));

        Map<String, Object> supportedTypes = new HashMap<>();
        supportedTypes.put("int8", (byte) 8);
        supportedTypes.put("int16", (short) 16);
        supportedTypes.put("int32", 32);
        supportedTypes.put("int64", (long) 64);
        supportedTypes.put("float32", 32.f);
        supportedTypes.put("float64", 64.);
        supportedTypes.put("boolean", true);
        supportedTypes.put("string", "stringy");
        supportedTypes.put("bytes", "bytes".getBytes());

        Map<String, Object> oneLevelNestedMap = Collections.singletonMap("B", supportedTypes);
        Map<String, Object> twoLevelNestedMap = Collections.singletonMap("A", oneLevelNestedMap);

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
                "topic", 0,
                null, twoLevelNestedMap));

        assertNull(transformed.valueSchema());
        assertInstanceOf(Map.class, transformed.value());
        @SuppressWarnings("unchecked")
        Map<String, Object> transformedMap = (Map<String, Object>) transformed.value();
        assertEquals(9, transformedMap.size());
        assertEquals((byte) 8, transformedMap.get("A#B#int8"));
        assertEquals((short) 16, transformedMap.get("A#B#int16"));
        assertEquals(32, transformedMap.get("A#B#int32"));
        assertEquals((long) 64, transformedMap.get("A#B#int64"));
        assertEquals(32.f, (float) transformedMap.get("A#B#float32"), 0.f);
        assertEquals(64., (double) transformedMap.get("A#B#float64"), 0.);
        assertEquals(true, transformedMap.get("A#B#boolean"));
        assertEquals("stringy", transformedMap.get("A#B#string"));
        assertArrayEquals("bytes".getBytes(), (byte[]) transformedMap.get("A#B#bytes"));
    }

    @Test
    public void testOptionalFieldStruct() {
        xformValue.configure(Collections.emptyMap());

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("opt_int32", Schema.OPTIONAL_INT32_SCHEMA);
        Schema supportedTypesSchema = builder.build();

        builder = SchemaBuilder.struct();
        builder.field("B", supportedTypesSchema);
        Schema oneLevelNestedSchema = builder.build();

        Struct supportedTypes = new Struct(supportedTypesSchema);
        supportedTypes.put("opt_int32", null);

        Struct oneLevelNestedStruct = new Struct(oneLevelNestedSchema);
        oneLevelNestedStruct.put("B", supportedTypes);

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
                "topic", 0,
                oneLevelNestedSchema, oneLevelNestedStruct));

        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type());
        Struct transformedStruct = (Struct) transformed.value();
        assertNull(transformedStruct.get("B.opt_int32"));
    }

    @Test
    public void testOptionalStruct() {
        xformValue.configure(Collections.emptyMap());

        SchemaBuilder builder = SchemaBuilder.struct().optional();
        builder.field("opt_int32", Schema.OPTIONAL_INT32_SCHEMA);
        Schema schema = builder.build();

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
            "topic", 0,
            schema, null));

        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type());
        assertNull(transformed.value());
    }

    @Test
    public void testOptionalNestedStruct() {
        xformValue.configure(Collections.emptyMap());

        SchemaBuilder builder = SchemaBuilder.struct().optional();
        builder.field("opt_int32", Schema.OPTIONAL_INT32_SCHEMA);
        Schema supportedTypesSchema = builder.build();

        builder = SchemaBuilder.struct();
        builder.field("B", supportedTypesSchema);
        Schema oneLevelNestedSchema = builder.build();

        Struct oneLevelNestedStruct = new Struct(oneLevelNestedSchema);
        oneLevelNestedStruct.put("B", null);

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
            "topic", 0,
            oneLevelNestedSchema, oneLevelNestedStruct));

        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type());
        Struct transformedStruct = (Struct) transformed.value();
        assertNull(transformedStruct.get("B.opt_int32"));
    }

    @Test
    public void testOptionalFieldMap() {
        xformValue.configure(Collections.emptyMap());

        Map<String, Object> supportedTypes = new HashMap<>();
        supportedTypes.put("opt_int32", null);

        Map<String, Object> oneLevelNestedMap = Collections.singletonMap("B", supportedTypes);

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
                "topic", 0,
                null, oneLevelNestedMap));

        assertNull(transformed.valueSchema());
        assertInstanceOf(Map.class, transformed.value());
        @SuppressWarnings("unchecked")
        Map<String, Object> transformedMap = (Map<String, Object>) transformed.value();

        assertNull(transformedMap.get("B.opt_int32"));
    }

    @Test
    public void testKey() {
        xformKey.configure(Collections.emptyMap());

        Map<String, Map<String, Integer>> key = Collections.singletonMap("A", Collections.singletonMap("B", 12));
        SourceRecord src = new SourceRecord(null, null, "topic", null, key, null, null);
        SourceRecord transformed = xformKey.apply(src);

        assertNull(transformed.keySchema());
        assertInstanceOf(Map.class, transformed.key());
        @SuppressWarnings("unchecked")
        Map<String, Object> transformedMap = (Map<String, Object>) transformed.key();
        assertEquals(12, transformedMap.get("A.B"));
    }

    @Test
    public void testSchemalessArray() {
        xformValue.configure(Collections.emptyMap());
        Object value = Collections.singletonMap("foo", Arrays.asList("bar", Collections.singletonMap("baz", Collections.singletonMap("lfg", "lfg"))));
        assertEquals(value, xformValue.apply(new SourceRecord(null, null, "topic", null, null, null, value)).value());
    }

    @Test
    public void testArrayWithSchema() {
        xformValue.configure(Collections.emptyMap());
        Schema nestedStructSchema = SchemaBuilder.struct().field("lfg", Schema.STRING_SCHEMA).build();
        Schema innerStructSchema = SchemaBuilder.struct().field("baz", nestedStructSchema).build();
        Schema structSchema = SchemaBuilder.struct()
            .field("foo", SchemaBuilder.array(innerStructSchema).doc("durk").build())
            .build();
        Struct nestedValue = new Struct(nestedStructSchema);
        nestedValue.put("lfg", "lfg");
        Struct innerValue = new Struct(innerStructSchema);
        innerValue.put("baz", nestedValue);
        Struct value = new Struct(structSchema);
        value.put("foo", Collections.singletonList(innerValue));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", null, null, structSchema, value)); 
        assertEquals(value, transformed.value());
        assertEquals(structSchema, transformed.valueSchema());
    }

    @Test
    public void testOptionalAndDefaultValuesNested() {
        // If we have a nested structure where an entire sub-Struct is optional, all flattened fields generated from its
        // children should also be optional. Similarly, if the parent Struct has a default value, the default value for
        // the flattened field

        xformValue.configure(Collections.emptyMap());

        SchemaBuilder builder = SchemaBuilder.struct().optional();
        builder.field("req_field", Schema.STRING_SCHEMA);
        builder.field("opt_field", SchemaBuilder.string().optional().defaultValue("child_default").build());
        Struct childDefaultValue = new Struct(builder);
        childDefaultValue.put("req_field", "req_default");
        builder.defaultValue(childDefaultValue);
        Schema schema = builder.build();
        // Intentionally leave this entire value empty since it is optional
        Struct value = new Struct(schema);

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, schema, value));

        assertNotNull(transformed);
        Schema transformedSchema = transformed.valueSchema();
        assertEquals(Schema.Type.STRUCT, transformedSchema.type());
        assertEquals(2, transformedSchema.fields().size());
        // Required field should pick up both being optional and the default value from the parent
        Schema transformedReqFieldSchema = SchemaBuilder.string().optional().defaultValue("req_default").build();
        assertEquals(transformedReqFieldSchema, transformedSchema.field("req_field").schema());
        // The optional field should still be optional but should have picked up the default value. However, since
        // the parent didn't specify the default explicitly, we should still be using the field's normal default
        Schema transformedOptFieldSchema = SchemaBuilder.string().optional().defaultValue("child_default").build();
        assertEquals(transformedOptFieldSchema, transformedSchema.field("opt_field").schema());
    }

    @Test
    public void tombstoneEventWithoutSchemaShouldPassThrough() {
        xformValue.configure(Collections.emptyMap());

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, null);
        final SourceRecord transformedRecord = xformValue.apply(record);

        assertNull(transformedRecord.value());
        assertNull(transformedRecord.valueSchema());
    }

    @Test
    public void tombstoneEventWithSchemaShouldPassThrough() {
        xformValue.configure(Collections.emptyMap());

        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA).build();
        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                simpleStructSchema, null);
        final SourceRecord transformedRecord = xformValue.apply(record);

        assertNull(transformedRecord.value());
        assertEquals(simpleStructSchema, transformedRecord.valueSchema());
    }

    @Test
    public void testMapWithNullFields() {
        xformValue.configure(Collections.emptyMap());

        // Use a LinkedHashMap to ensure the SMT sees entries in a specific order
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("firstNull", null);
        value.put("firstNonNull", "nonNull");
        value.put("secondNull", null);
        value.put("secondNonNull", "alsoNonNull");
        value.put("thirdNonNull", null);

        final SourceRecord record = new SourceRecord(null, null, "test", 0, null, value);
        final SourceRecord transformedRecord = xformValue.apply(record);

        assertEquals(value, transformedRecord.value());
    }

    @Test
    public void testStructWithNullFields() {
        xformValue.configure(Collections.emptyMap());

        final Schema structSchema = SchemaBuilder.struct()
            .field("firstNull", Schema.OPTIONAL_STRING_SCHEMA)
            .field("firstNonNull", Schema.OPTIONAL_STRING_SCHEMA)
            .field("secondNull", Schema.OPTIONAL_STRING_SCHEMA)
            .field("secondNonNull", Schema.OPTIONAL_STRING_SCHEMA)
            .field("thirdNonNull", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

        final Struct value = new Struct(structSchema);
        value.put("firstNull", null);
        value.put("firstNonNull", "nonNull");
        value.put("secondNull", null);
        value.put("secondNonNull", "alsoNonNull");
        value.put("thirdNonNull", null);

        final SourceRecord record = new SourceRecord(null, null, "test", 0, structSchema, value);
        final SourceRecord transformedRecord = xformValue.apply(record);

        assertEquals(value, transformedRecord.value());
    }

    @Test
    public void testFlattenVersionRetrievedFromAppInfoParser() {
        assertEquals(AppInfoParser.getVersion(), xformKey.version());
        assertEquals(AppInfoParser.getVersion(), xformValue.version());

        assertEquals(xformKey.version(), xformValue.version());
    }
}
