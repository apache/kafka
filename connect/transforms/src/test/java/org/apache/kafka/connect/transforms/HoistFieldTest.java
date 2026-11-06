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

import org.apache.kafka.common.utils.internals.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.field.FieldSyntaxVersion;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HoistFieldTest {
    private final HoistField<SinkRecord> xform = new HoistField.Key<>();

    @AfterEach
    void teardown() {
        xform.close();
    }

    private static Map<String, String> v2(String field, String hoisted) {
        Map<String, String> configs = new HashMap<>();
        configs.put("field", field);
        configs.put("hoisted", hoisted);
        configs.put(FieldSyntaxVersion.FIELD_SYNTAX_VERSION_CONFIG, FieldSyntaxVersion.V2.name());
        return configs;
    }

    @Test
    @DisplayName("Schemaless: empty hoisted wraps whole value in a single-field Map")
    void schemaless() {
        xform.configure(Map.of("field", "magic"));

        final SinkRecord sinkRecord = new SinkRecord("test", 0, null, 42, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(sinkRecord);

        assertNull(transformedRecord.keySchema());
        assertEquals(Map.of("magic", 42), transformedRecord.key());
    }

    @Test
    @DisplayName("With schema: empty hoisted wraps whole value in a single-field Struct")
    void withSchema() {
        xform.configure(Map.of("field", "magic"));

        final SinkRecord sinkRecord = new SinkRecord("test", 0, Schema.INT32_SCHEMA, 42, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(sinkRecord);

        assertEquals(Schema.Type.STRUCT, transformedRecord.keySchema().type());
        assertEquals(sinkRecord.keySchema(), transformedRecord.keySchema().field("magic").schema());
        assertEquals(42, ((Struct) transformedRecord.key()).get("magic"));
    }

    @Test
    @DisplayName("Schemaless: resulting wrapper Map is mutable")
    void testSchemalessMapIsMutable() {
        xform.configure(Map.of("field", "magic"));

        final SinkRecord sinkRecord = new SinkRecord("test", 0, null, 420, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(sinkRecord);

        assertNull(transformedRecord.keySchema());
        @SuppressWarnings("unchecked")
        Map<String, Object> actualKey = (Map<String, Object>) transformedRecord.key();
        actualKey.put("k", "v");
        Map<String, Object> expectedKey = new HashMap<>();
        expectedKey.put("k", "v");
        expectedKey.put("magic", 420);
        assertEquals(expectedKey, actualKey);
    }

    @Test
    @DisplayName("version() returns AppInfoParser version")
    void testHoistFieldVersionRetrievedFromAppInfoParser() {
        assertEquals(AppInfoParser.getVersion(), xform.version());
    }

    @Test
    @DisplayName("V2 with empty hoisted behaves like classic hoist")
    void emptyHoistedV2WrapsWholeValue() {
        xform.configure(v2("magic", ""));

        final SinkRecord sinkRecord = new SinkRecord("test", 0, Schema.INT32_SCHEMA, 42, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(sinkRecord);

        assertEquals(Schema.Type.STRUCT, transformedRecord.keySchema().type());
        assertEquals(sinkRecord.keySchema(), transformedRecord.keySchema().field("magic").schema());
        assertEquals(42, ((Struct) transformedRecord.key()).get("magic"));
    }

    @Test
    @DisplayName("Null hoisted value is treated as empty (classic hoist)")
    void nullHoistedTreatedAsEmpty() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put("field", "magic");
        configs.put("hoisted", null);
        xform.configure(configs);

        final SinkRecord sinkRecord = new SinkRecord("test", 0, null, 42, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(sinkRecord);

        assertNull(transformedRecord.keySchema());
        assertEquals(Map.of("magic", 42), transformedRecord.key());
    }

    @Test
    @DisplayName("V2 schemaless: hoists nested element, preserving siblings")
    void schemalessV2() {
        final HoistField<SinkRecord> valueXform = new HoistField.Value<>();
        valueXform.configure(v2("other", "parent.child.k2"));

        final Map<String, Object> child = new HashMap<>();
        child.put("k2", "123");
        final Map<String, Object> parent = new HashMap<>();
        parent.put("child", child);
        final Map<String, Object> value = new HashMap<>();
        value.put("k1", 123);
        value.put("parent", parent);

        final SinkRecord sinkRecord = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = valueXform.apply(sinkRecord);

        final Map<String, Object> expectedChild = Map.of("other", Map.of("k2", "123"));
        final Map<String, Object> expected = Map.of("k1", 123, "parent", Map.of("child", expectedChild));
        assertEquals(expected, transformedRecord.value());

        valueXform.close();
    }

    @Test
    @DisplayName("V2 with schema: hoists nested element, preserving siblings")
    void schemaV2() {
        final HoistField<SinkRecord> valueXform = new HoistField.Value<>();
        valueXform.configure(v2("other", "parent.child.k2"));

        final Schema childSchema = SchemaBuilder.struct().field("k2", Schema.STRING_SCHEMA).build();
        final Schema parentSchema = SchemaBuilder.struct().field("child", childSchema).build();
        final Schema rootSchema = SchemaBuilder.struct()
                .field("k1", Schema.INT32_SCHEMA)
                .field("parent", parentSchema)
                .build();
        final Struct root = new Struct(rootSchema)
                .put("k1", 123)
                .put("parent", new Struct(parentSchema)
                        .put("child", new Struct(childSchema).put("k2", "123")));

        final SinkRecord sinkRecord = new SinkRecord("test", 0, null, null, rootSchema, root, 0);
        final SinkRecord transformedRecord = valueXform.apply(sinkRecord);

        final Struct out = (Struct) transformedRecord.value();
        assertEquals(123, out.get("k1"));
        final Struct parent = (Struct) out.get("parent");
        final Struct child = (Struct) parent.get("child");
        final Struct other = assertInstanceOf(Struct.class, child.get("other"));
        assertEquals("123", other.get("k2"));

        // schema mirrors the wrapped value and preserves the original leaf schema
        final Schema outSchema = transformedRecord.valueSchema();
        assertEquals(Schema.INT32_SCHEMA, outSchema.field("k1").schema());
        final Schema otherSchema = outSchema.field("parent").schema().field("child").schema().field("other").schema();
        assertEquals(Schema.STRING_SCHEMA, otherSchema.field("k2").schema());

        valueXform.close();
    }

    @Test
    @DisplayName("V2 schemaless: hoists a field whose name contains a dot")
    void escapedDotsSchemaless() {
        final HoistField<SinkRecord> valueXform = new HoistField.Value<>();
        valueXform.configure(v2("other", "`parent.child`"));

        final Map<String, Object> value = new HashMap<>();
        value.put("k1", 123);
        value.put("parent.child", Map.of("k2", "123"));

        final SinkRecord sinkRecord = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = valueXform.apply(sinkRecord);

        final Map<String, Object> expected = Map.of(
                "k1", 123,
                "other", Map.of("parent.child", Map.of("k2", "123")));
        assertEquals(expected, transformedRecord.value());

        valueXform.close();
    }

    @Test
    @DisplayName("V2 with schema: KIP example 2 hoists a field whose name contains a dot")
    void escapedDotsWithSchema() {
        final HoistField<SinkRecord> valueXform = new HoistField.Value<>();
        valueXform.configure(v2("other", "`parent.child`"));

        final Schema pcSchema = SchemaBuilder.struct().field("k2", Schema.STRING_SCHEMA).build();
        final Schema rootSchema = SchemaBuilder.struct()
                .field("k1", Schema.INT32_SCHEMA)
                .field("parent.child", pcSchema)
                .build();
        final Struct root = new Struct(rootSchema)
                .put("k1", 123)
                .put("parent.child", new Struct(pcSchema).put("k2", "123"));

        final SinkRecord sinkRecord = new SinkRecord("test", 0, null, null, rootSchema, root, 0);
        final SinkRecord transformedRecord = valueXform.apply(sinkRecord);

        final Struct out = (Struct) transformedRecord.value();
        assertEquals(123, out.get("k1"));
        final Struct other = assertInstanceOf(Struct.class, out.get("other"));
        final Struct pc = (Struct) other.get("parent.child");
        assertEquals("123", pc.get("k2"));

        valueXform.close();
    }

    @Test
    @DisplayName("V2 schemaless: single-level hoisted wraps a root field, preserving siblings")
    void singleLevelHoistedSchemaless() {
        xform.configure(v2("other", "k2"));

        final Map<String, Object> value = new HashMap<>();
        value.put("k1", 123);
        value.put("k2", "New York");

        final SinkRecord sinkRecord = new SinkRecord("test", 0, null, value, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(sinkRecord);

        assertEquals(Map.of("k1", 123, "other", Map.of("k2", "New York")), transformedRecord.key());
    }

    @Test
    @DisplayName("V2 with schema: hoisting an intermediate node wraps the whole subtree")
    void intermediateNodeHoistedWithSchema() {
        xform.configure(v2("other", "parent"));

        final Schema childSchema = SchemaBuilder.struct().field("k2", Schema.STRING_SCHEMA).build();
        final Schema parentSchema = SchemaBuilder.struct().field("child", childSchema).build();
        final Schema rootSchema = SchemaBuilder.struct()
                .field("k1", Schema.INT32_SCHEMA)
                .field("parent", parentSchema)
                .build();
        final Struct root = new Struct(rootSchema)
                .put("k1", 123)
                .put("parent", new Struct(parentSchema)
                        .put("child", new Struct(childSchema).put("k2", "123")));

        final SinkRecord sinkRecord = new SinkRecord("test", 0, rootSchema, root, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(sinkRecord);

        final Struct out = (Struct) transformedRecord.key();
        assertEquals(123, out.get("k1"));
        final Struct other = assertInstanceOf(Struct.class, out.get("other"));
        final Struct parent = (Struct) other.get("parent");
        final Struct child = (Struct) parent.get("child");
        assertEquals("123", child.get("k2"));
    }

    @Test
    @DisplayName("V1: dotted hoisted path is treated as a single literal field name")
    void v1DottedHoistedIsLiteral() {
        Map<String, String> configs = new HashMap<>();
        configs.put("field", "other");
        configs.put("hoisted", "a.b.c");
        // V1 is the default
        xform.configure(configs);

        final Map<String, Object> value = new HashMap<>();
        value.put("k1", 123);
        value.put("a.b.c", 42);

        final SinkRecord sinkRecord = new SinkRecord("test", 0, null, value, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(sinkRecord);

        assertEquals(Map.of("k1", 123, "other", Map.of("a.b.c", 42)), transformedRecord.key());
    }

    @Test
    @DisplayName("V2 schemaless: result and inner wrapper Maps are mutable")
    @SuppressWarnings("unchecked")
    void hoistedSchemalessResultIsMutable() {
        xform.configure(v2("other", "parent.k2"));

        final Map<String, Object> parent = new LinkedHashMap<>();
        parent.put("k2", "NY");
        final Map<String, Object> value = new LinkedHashMap<>();
        value.put("k1", 123);
        value.put("parent", parent);

        final SinkRecord sinkRecord = new SinkRecord("test", 0, null, value, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(sinkRecord);

        Map<String, Object> out = (Map<String, Object>) transformedRecord.key();
        out.put("extra", "value");
        Map<String, Object> outParent = (Map<String, Object>) out.get("parent");
        Map<String, Object> wrapper = (Map<String, Object>) outParent.get("other");
        wrapper.put("added", 1);

        assertEquals("NY", wrapper.get("k2"));
        assertEquals(1, wrapper.get("added"));
        assertEquals("value", out.get("extra"));
        assertEquals(123, out.get("k1"));
    }

    @Test
    @DisplayName("With schema: unknown hoisted path throws")
    void unknownHoistedPathWithSchemaThrows() {
        xform.configure(v2("other", "does.not.exist"));

        final Schema rootSchema = SchemaBuilder.struct().field("k1", Schema.INT32_SCHEMA).build();
        final Struct root = new Struct(rootSchema).put("k1", 123);
        final SinkRecord sinkRecord = new SinkRecord("test", 0, rootSchema, root, null, null, 0);

        assertThrows(IllegalArgumentException.class, () -> xform.apply(sinkRecord));
    }

    @Test
    @DisplayName("Schemaless: unknown hoisted path throws")
    void unknownHoistedPathSchemalessThrows() {
        xform.configure(v2("other", "does.not.exist"));

        final Map<String, Object> value = new HashMap<>();
        value.put("k1", 123);
        final SinkRecord sinkRecord = new SinkRecord("test", 0, null, value, null, null, 0);

        assertThrows(IllegalArgumentException.class, () -> xform.apply(sinkRecord));
    }
}
