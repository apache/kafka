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
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.field.FieldSyntaxVersion;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

public class ExtractFieldTest {
    private final ExtractField<SinkRecord> xformKey = new ExtractField.Key<>();
    private final ExtractField<SinkRecord> xformValue = new ExtractField.Value<>();

    public static Stream<Arguments> data() {
        return Stream.of(
                Arguments.of(false, null),
                Arguments.of(true, 42)
        );
    }

    @AfterEach
    public void teardown() {
        xformKey.close();
        xformValue.close();
    }

    @Test
    public void schemaless() {
        xformKey.configure(Map.of("field", "magic"));

        final SinkRecord record = new SinkRecord("test", 0, null, Map.of("magic", 42), null, null, 0);
        final SinkRecord transformedRecord = xformKey.apply(record);

        assertNull(transformedRecord.keySchema());
        assertEquals(42, transformedRecord.key());
    }

    @Test
    public void schemalessAndNestedPath() {
        Map<String, String> configs = new HashMap<>();
        configs.put(FieldSyntaxVersion.FIELD_SYNTAX_VERSION_CONFIG, FieldSyntaxVersion.V2.name());
        configs.put("field", "magic.foo");
        xformKey.configure(configs);

        final Map<String, Object> key = Map.of("magic", Map.of("foo", 42));
        final SinkRecord record = new SinkRecord("test", 0, null, key, null, null, 0);
        final SinkRecord transformedRecord = xformKey.apply(record);

        assertNull(transformedRecord.keySchema());
        assertEquals(42, transformedRecord.key());
    }

    @Test
    public void nullSchemaless() {
        xformKey.configure(Map.of("field", "magic"));

        final Map<String, Object> key = null;
        final SinkRecord record = new SinkRecord("test", 0, null, key, null, null, 0);
        final SinkRecord transformedRecord = xformKey.apply(record);

        assertNull(transformedRecord.keySchema());
        assertNull(transformedRecord.key());
    }

    @Test
    public void withSchema() {
        xformKey.configure(Map.of("field", "magic"));

        final Schema keySchema = SchemaBuilder.struct().field("magic", Schema.INT32_SCHEMA).build();
        final Struct key = new Struct(keySchema).put("magic", 42);
        final SinkRecord record = new SinkRecord("test", 0, keySchema, key, null, null, 0);
        final SinkRecord transformedRecord = xformKey.apply(record);

        assertEquals(Schema.INT32_SCHEMA, transformedRecord.keySchema());
        assertEquals(42, transformedRecord.key());
    }

    @Test
    public void withSchemaAndNestedPath() {
        Map<String, String> configs = new HashMap<>();
        configs.put(FieldSyntaxVersion.FIELD_SYNTAX_VERSION_CONFIG, FieldSyntaxVersion.V2.name());
        configs.put("field", "magic.foo");
        xformKey.configure(configs);

        final Schema fooSchema = SchemaBuilder.struct().field("foo", Schema.INT32_SCHEMA).build();
        final Schema keySchema = SchemaBuilder.struct().field("magic", fooSchema).build();
        final Struct key = new Struct(keySchema).put("magic", new Struct(fooSchema).put("foo", 42));
        final SinkRecord record = new SinkRecord("test", 0, keySchema, key, null, null, 0);
        final SinkRecord transformedRecord = xformKey.apply(record);

        assertEquals(Schema.INT32_SCHEMA, transformedRecord.keySchema());
        assertEquals(42, transformedRecord.key());
    }

    @Test
    public void testNullWithSchema() {
        xformKey.configure(Map.of("field", "magic"));

        final Schema keySchema = SchemaBuilder.struct().field("magic", Schema.INT32_SCHEMA).optional().build();
        final Struct key = null;
        final SinkRecord record = new SinkRecord("test", 0, keySchema, key, null, null, 0);
        final SinkRecord transformedRecord = xformKey.apply(record);

        assertEquals(Schema.INT32_SCHEMA, transformedRecord.keySchema());
        assertNull(transformedRecord.key());
    }

    @Test
    public void nonExistentFieldSchemalessShouldReturnNull() {
        xformKey.configure(Map.of("field", "nonexistent"));

        final SinkRecord record = new SinkRecord("test", 0, null, Map.of("magic", 42), null, null, 0);
        final SinkRecord transformedRecord = xformKey.apply(record);

        assertNull(transformedRecord.keySchema());
        assertNull(transformedRecord.key());
    }

    @Test
    public void nonExistentNestedFieldSchemalessShouldReturnNull() {
        Map<String, String> configs = new HashMap<>();
        configs.put(FieldSyntaxVersion.FIELD_SYNTAX_VERSION_CONFIG, FieldSyntaxVersion.V2.name());
        configs.put("field", "magic.nonexistent");
        xformKey.configure(configs);

        final Map<String, Object> key = Map.of("magic", Map.of("foo", 42));
        final SinkRecord record = new SinkRecord("test", 0, null, key, null, null, 0);
        final SinkRecord transformedRecord = xformKey.apply(record);

        assertNull(transformedRecord.keySchema());
        assertNull(transformedRecord.key());
    }

    @Test
    public void nonExistentFieldWithSchemaShouldFail() {
        xformKey.configure(Map.of("field", "nonexistent"));

        final Schema keySchema = SchemaBuilder.struct().field("magic", Schema.INT32_SCHEMA).build();
        final Struct key = new Struct(keySchema).put("magic", 42);
        final SinkRecord record = new SinkRecord("test", 0, keySchema, key, null, null, 0);

        try {
            xformKey.apply(record);
            fail("Expected exception wasn't raised");
        } catch (IllegalArgumentException iae) {
            assertEquals("Unknown field: nonexistent", iae.getMessage());
        }
    }

    @Test
    public void nonExistentNestedFieldWithSchemaShouldFail() {
        Map<String, String> configs = new HashMap<>();
        configs.put(FieldSyntaxVersion.FIELD_SYNTAX_VERSION_CONFIG, FieldSyntaxVersion.V2.name());
        configs.put("field", "magic.nonexistent");
        xformKey.configure(configs);

        final Schema fooSchema = SchemaBuilder.struct().field("foo", Schema.INT32_SCHEMA).build();
        final Schema keySchema = SchemaBuilder.struct().field("magic", fooSchema).build();
        final Struct key = new Struct(keySchema).put("magic", new Struct(fooSchema).put("foo", 42));
        final SinkRecord record = new SinkRecord("test", 0, keySchema, key, null, null, 0);

        try {
            xformKey.apply(record);
            fail("Expected exception wasn't raised");
        } catch (IllegalArgumentException iae) {
            assertEquals("Unknown field: magic.nonexistent", iae.getMessage());
        }
    }

    @Test
    public void testExtractFieldVersionRetrievedFromAppInfoParser() {
        assertEquals(AppInfoParser.getVersion(), xformKey.version());
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUnsetOptionalKey(boolean replaceNullWithDefault, Object expectedValue) {

        Map<String, Object> config = new HashMap<>();
        config.put("field", "optional_with_default");
        config.put("replace.null.with.default", replaceNullWithDefault);

        xformKey.configure(config);

        final Schema keySchema = SchemaBuilder.struct()
                .field("optional_with_default", SchemaBuilder.int32().optional().defaultValue(42).build())
                .build();
        final Struct key = new Struct(keySchema).put("optional_with_default", null);

        final SinkRecord record = new SinkRecord("test", 0, keySchema, key, null, null, 0);

        final SinkRecord transformedRecord = xformKey.apply(record);
        Integer extractedValue = (Integer) transformedRecord.key();

        assertEquals(expectedValue, extractedValue);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUnsetOptionalField(boolean replaceNullWithDefault, Object expectedValue) {

        Map<String, Object> config = new HashMap<>();
        config.put("field", "optional_with_default");
        config.put("replace.null.with.default", replaceNullWithDefault);

        xformValue.configure(config);

        final Schema valueSchema = SchemaBuilder.struct()
                .field("optional_with_default", SchemaBuilder.int32().optional().defaultValue(42).build())
                .build();
        final Struct value = new Struct(valueSchema).put("optional_with_default", null);

        final SinkRecord record = new SinkRecord("test", 0, null, null, valueSchema, value, 0);

        final SinkRecord transformedRecord = xformValue.apply(record);
        Integer extractedValue = (Integer) transformedRecord.value();

        assertEquals(expectedValue, extractedValue);
    }

}
