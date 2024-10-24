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

import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

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
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ReplaceFieldTest {
    private final ReplaceField<SinkRecord> xformKey = new ReplaceField.Key<>();
    private final ReplaceField<SinkRecord> xform = new ReplaceField.Value<>();

    public static Stream<Arguments> data() {
        return Stream.of(
                Arguments.of(false, null),
                Arguments.of(true, 42)
        );
    }

    @AfterEach
    public void teardown() {
        xform.close();
    }

    @Test
    public void tombstoneSchemaless() {
        final Map<String, String> props = new HashMap<>();
        props.put("include", "abc,foo");
        props.put("renames", "abc:xyz,foo:bar");

        xform.configure(props);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.value());
        assertNull(transformedRecord.valueSchema());
    }

    @Test
    public void tombstoneWithSchema() {
        final Map<String, String> props = new HashMap<>();
        props.put("include", "abc,foo");
        props.put("renames", "abc:xyz,foo:bar");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
            .field("dont", Schema.STRING_SCHEMA)
            .field("abc", Schema.INT32_SCHEMA)
            .field("foo", Schema.BOOLEAN_SCHEMA)
            .field("etc", Schema.STRING_SCHEMA)
            .build();

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.value());
        assertEquals(schema, transformedRecord.valueSchema());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void schemaless() {
        final Map<String, String> props = new HashMap<>();
        props.put("exclude", "dont");
        props.put("renames", "abc:xyz,foo:bar");

        xform.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("dont", "whatever");
        value.put("abc", 42);
        value.put("foo", true);
        value.put("etc", "etc");

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Map<String, Object> updatedValue = (Map<String, Object>) transformedRecord.value();
        assertEquals(3, updatedValue.size());
        assertEquals(42, updatedValue.get("xyz"));
        assertEquals(true, updatedValue.get("bar"));
        assertEquals("etc", updatedValue.get("etc"));
    }

    @Test
    public void withSchema() {
        final Map<String, String> props = new HashMap<>();
        props.put("include", "abc,foo");
        props.put("renames", "abc:xyz,foo:bar");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("dont", Schema.STRING_SCHEMA)
                .field("abc", Schema.INT32_SCHEMA)
                .field("foo", Schema.BOOLEAN_SCHEMA)
                .field("etc", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("dont", "whatever");
        value.put("abc", 42);
        value.put("foo", true);
        value.put("etc", "etc");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        assertEquals(2, updatedValue.schema().fields().size());
        assertEquals(Integer.valueOf(42), updatedValue.getInt32("xyz"));
        assertEquals(true, updatedValue.getBoolean("bar"));
    }

    @Test
    public void testReplaceFieldVersionRetrievedFromAppInfoParser() {
        assertEquals(AppInfoParser.getVersion(), xform.version());
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testReplaceNullWithDefaultConfigOnValue(boolean replaceNullWithDefault, Object expectedValue) {
        final Map<String, String> props = new HashMap<>();
        props.put("include", "abc");
        props.put("renames", "abc:optional_with_default");
        props.put("replace.null.with.default", String.valueOf(replaceNullWithDefault));

        xform.configure(props);

        final Schema valueSchema = SchemaBuilder.struct()
                .field("abc", SchemaBuilder.int32().optional().defaultValue(42).build())
                .build();

        final Struct value = new Struct(valueSchema).put("abc", null);

        final SinkRecord record = new SinkRecord("test", 0, null, null, valueSchema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        assertEquals(1, updatedValue.schema().fields().size());
        assertEquals(expectedValue, updatedValue.getWithoutDefault("optional_with_default"));
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testReplaceNullWithDefaultConfigOnKey(boolean replaceNullWithDefault, Object expectedValue) {
        final Map<String, String> props = new HashMap<>();
        props.put("include", "abc");
        props.put("renames", "abc:optional_with_default");
        props.put("replace.null.with.default", String.valueOf(replaceNullWithDefault));

        xformKey.configure(props);

        final Schema keySchema = SchemaBuilder.struct()
                .field("abc", SchemaBuilder.int32().optional().defaultValue(42).build())
                .build();

        final Struct key = new Struct(keySchema).put("abc", null);

        final SinkRecord record = new SinkRecord("test", 0, keySchema, key, null, null, 0);
        final SinkRecord transformedRecord = xformKey.apply(record);

        final Struct updatedKey = (Struct) transformedRecord.key();

        assertEquals(1, updatedKey.schema().fields().size());
        assertEquals(expectedValue, updatedKey.getWithoutDefault("optional_with_default"));
    }

    @Test
    public void testInvalidConfig() {
        final Map<String, String> props = new HashMap<>();
        props.put("invalidConfig", "dont");

        assertThrows(InvalidConfigurationException.class, () -> xform.configure(props));
    }
}
