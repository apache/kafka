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
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SetSchemaMetadataTest {
    private final SetSchemaMetadata<SinkRecord> xform = new SetSchemaMetadata.Value<>();

    public static Stream<Arguments> data() {
        return Stream.of(
                Arguments.of(false, null),
                Arguments.of(true, "default")
        );
    }

    @AfterEach
    public void teardown() {
        xform.close();
    }

    @Test
    public void schemaNameUpdate() {
        xform.configure(Map.of("schema.name", "foo"));
        final SinkRecord record = new SinkRecord("", 0, null, null, SchemaBuilder.struct().build(), null, 0);
        final SinkRecord updatedRecord = xform.apply(record);
        assertEquals("foo", updatedRecord.valueSchema().name());
    }

    @Test
    public void schemaVersionUpdate() {
        xform.configure(Map.of("schema.version", 42));
        final SinkRecord record = new SinkRecord("", 0, null, null, SchemaBuilder.struct().build(), null, 0);
        final SinkRecord updatedRecord = xform.apply(record);
        assertEquals(42, updatedRecord.valueSchema().version());
    }

    @Test
    public void schemaNameAndVersionUpdate() {
        final Map<String, String> props = new HashMap<>();
        props.put("schema.name", "foo");
        props.put("schema.version", "42");

        xform.configure(props);

        final SinkRecord record = new SinkRecord("", 0, null, null, SchemaBuilder.struct().build(), null, 0);

        final SinkRecord updatedRecord = xform.apply(record);

        assertEquals("foo", updatedRecord.valueSchema().name());
        assertEquals(42, updatedRecord.valueSchema().version());
    }

    @Test
    public void schemaNameAndVersionUpdateWithStruct() {
        final String fieldName1 = "f1";
        final String fieldName2 = "f2";
        final String fieldValue1 = "value1";
        final int fieldValue2 = 1;
        final Schema schema = SchemaBuilder.struct()
                                      .name("my.orig.SchemaDefn")
                                      .field(fieldName1, Schema.STRING_SCHEMA)
                                      .field(fieldName2, Schema.INT32_SCHEMA)
                                      .build();
        final Struct value = new Struct(schema).put(fieldName1, fieldValue1).put(fieldName2, fieldValue2);

        final Map<String, String> props = new HashMap<>();
        props.put("schema.name", "foo");
        props.put("schema.version", "42");
        xform.configure(props);

        final SinkRecord record = new SinkRecord("", 0, null, null, schema, value, 0);

        final SinkRecord updatedRecord = xform.apply(record);

        assertEquals("foo", updatedRecord.valueSchema().name());
        assertEquals(42, updatedRecord.valueSchema().version());

        // Make sure the struct's schema and fields all point to the new schema
        assertMatchingSchema((Struct) updatedRecord.value(), updatedRecord.valueSchema());
    }

    @ParameterizedTest
    @MethodSource("data")
    public void schemaNameAndVersionUpdateWithStructAndNullField(boolean replaceNullWithDefault, Object expectedValue) {
        final String fieldName1 = "f1";
        final String fieldName2 = "f2";
        final int fieldValue2 = 1;
        final Schema schema = SchemaBuilder.struct()
                .name("my.orig.SchemaDefn")
                .field(fieldName1, SchemaBuilder.string().defaultValue("default").optional().build())
                .field(fieldName2, Schema.INT32_SCHEMA)
                .build();
        final Struct value = new Struct(schema).put(fieldName1, null).put(fieldName2, fieldValue2);

        final Map<String, Object> props = new HashMap<>();
        props.put("schema.name", "foo");
        props.put("schema.version", "42");
        props.put("replace.null.with.default", replaceNullWithDefault);
        xform.configure(props);

        final SinkRecord record = new SinkRecord("", 0, null, null, schema, value, 0);

        final SinkRecord updatedRecord = xform.apply(record);

        assertEquals("foo", updatedRecord.valueSchema().name());
        assertEquals(42, updatedRecord.valueSchema().version());

        // Make sure the struct's schema and fields all point to the new schema
        assertMatchingSchema((Struct) updatedRecord.value(), updatedRecord.valueSchema());

        assertEquals(expectedValue, ((Struct) updatedRecord.value()).getWithoutDefault(fieldName1));
    }

    @Test
    public void valueSchemaRequired() {
        final SinkRecord record = new SinkRecord("", 0, null, null, null, 42, 0);
        assertThrows(DataException.class, () -> xform.apply(record));
    }

    @Test
    public void ignoreRecordWithNullValue() {
        final SinkRecord record = new SinkRecord("", 0, null, null, null, null, 0);

        final SinkRecord updatedRecord = xform.apply(record);

        assertNull(updatedRecord.key());
        assertNull(updatedRecord.keySchema());
        assertNull(updatedRecord.value());
        assertNull(updatedRecord.valueSchema());
    }

    @Test
    public void updateSchemaOfStruct() {
        final String fieldName1 = "f1";
        final String fieldName2 = "f2";
        final String fieldValue1 = "value1";
        final int fieldValue2 = 1;
        final Schema schema = SchemaBuilder.struct()
                                      .name("my.orig.SchemaDefn")
                                      .field(fieldName1, Schema.STRING_SCHEMA)
                                      .field(fieldName2, Schema.INT32_SCHEMA)
                                      .build();
        final Struct value = new Struct(schema).put(fieldName1, fieldValue1).put(fieldName2, fieldValue2);

        final Schema newSchema = SchemaBuilder.struct()
                                      .name("my.updated.SchemaDefn")
                                      .field(fieldName1, Schema.STRING_SCHEMA)
                                      .field(fieldName2, Schema.INT32_SCHEMA)
                                      .build();

        Struct newValue = (Struct) xform.updateSchemaIn(value, newSchema);
        assertMatchingSchema(newValue, newSchema);
    }

    @Test
    public void updateSchemaOfNonStruct() {
        Object value = 1;
        Object updatedValue = xform.updateSchemaIn(value, Schema.INT32_SCHEMA);
        assertSame(value, updatedValue);
    }

    @Test
    public void updateSchemaOfNull() {
        Object updatedValue = xform.updateSchemaIn(null, Schema.INT32_SCHEMA);
        assertNull(updatedValue);
    }

    @Test
    public void testSchemaMetadataVersionRetrievedFromAppInfoParser() {
        assertEquals(AppInfoParser.getVersion(), xform.version());
    }

    protected void assertMatchingSchema(Struct value, Schema schema) {
        assertSame(schema, value.schema());
        assertEquals(schema.name(), value.schema().name());
        for (Field field : schema.fields()) {
            String fieldName = field.name();
            assertEquals(schema.field(fieldName).name(), value.schema().field(fieldName).name());
            assertEquals(schema.field(fieldName).index(), value.schema().field(fieldName).index());
            assertSame(schema.field(fieldName).schema(), value.schema().field(fieldName).schema());
        }
    }
}
