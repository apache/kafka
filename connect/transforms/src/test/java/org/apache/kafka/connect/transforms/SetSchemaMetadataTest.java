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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SetSchemaMetadataTest {
    private final SetSchemaMetadata<SinkRecord> xform = new SetSchemaMetadata.Value<>();

    private static Stream<Arguments> buildSchemaNameTestParams() {
        return Stream.of(
                Arguments.of("foo.bar", "", "foo.bar"),
                Arguments.of("foo.bar", null, "foo.bar"),
                Arguments.of("foo.bar.", "", "foo.bar"),
                Arguments.of("foo.bar...", "", "foo.bar"),
                Arguments.of(".foo.bar", "", ".foo.bar"),
                Arguments.of("  foo.bar.  ", "", "foo.bar"),
                Arguments.of("", "baz", "baz"),
                Arguments.of(null, "baz", "baz"),
                Arguments.of("", ".baz", ".baz"),
                Arguments.of("", "...baz ", "...baz "),
                Arguments.of("", " baz ", " baz "),
                Arguments.of("foo.bar", "baz", "foo.bar.baz"),
                Arguments.of("foo.bar", ".baz", "foo.bar.baz"),
                Arguments.of("foo.bar", "...baz ", "foo.bar.baz"),
                Arguments.of("foo.bar", "  .baz  ", "foo.bar.baz"),
                Arguments.of("foo.bar", "baz.", "foo.bar.baz."),
                Arguments.of("foo.bar", "foo.bar.baz", "foo.bar.foo.bar.baz"),
                Arguments.of("", "", ""),
                Arguments.of(null, "", ""),
                Arguments.of("", null, null),
                Arguments.of(null, null, null)
        );
    }

    @AfterEach
    public void teardown() {
        xform.close();
    }

    @ParameterizedTest
    @MethodSource("buildSchemaNameTestParams")
    public void buildSchemaNameShouldBeTolerantToRedundantDotsAndSpaces(String namespace,
                                                                        String schemaName,
                                                                        String expected) {
        String actual = SetSchemaMetadata.buildSchemaName(namespace, schemaName);
        assertEquals(expected, actual);
    }

    @Test
    public void shouldTakeSchemaNameFromConfig() {
        final SinkRecord record = buildRecordAndApplyConfig(
                SchemaBuilder.struct().build(), Collections.singletonMap("schema.name", "foo")
        );

        assertEquals("foo", record.valueSchema().name());
    }

    @Test
    public void shouldTakeSchemaNamespaceFromConfig() {
        final SinkRecord record = buildRecordAndApplyConfig(
                SchemaBuilder.struct().build(), Collections.singletonMap("schema.namespace", "foo.bar")
        );

        assertEquals("foo.bar", record.valueSchema().name());
    }

    @Test
    public void shouldTakeSchemaNameWithNamespaceFromConfig() {
        final Map<String, String> config = new HashMap<>();
        config.put("schema.namespace", "foo.bar");
        config.put("schema.name", "baz");
        Schema schema = SchemaBuilder.struct().build();

        final SinkRecord record = buildRecordAndApplyConfig(schema, config);

        assertEquals("foo.bar.baz", record.valueSchema().name());
    }

    @Test
    public void shouldTakeSchemaNameWithNamespaceAndVersionFromConfig() {
        final Map<String, String> config = new HashMap<>();
        config.put("schema.namespace", "foo.bar");
        config.put("schema.name", "baz");
        config.put("schema.version", "42");
        Schema schema = SchemaBuilder.struct().build();

        final SinkRecord record = buildRecordAndApplyConfig(schema, config);

        assertEquals("foo.bar.baz", record.valueSchema().name());
        assertEquals(42, record.valueSchema().version());
    }

    @Test
    public void shouldPrependNamespaceFromConfigToCurrentSchemaName() {
        final SinkRecord record = buildRecordAndApplyConfig(
                SchemaBuilder.struct().name("baz").build(), Collections.singletonMap("schema.namespace", "foo.bar")
        );

        assertEquals("foo.bar.baz", record.valueSchema().name());
    }

    @Test
    public void shouldOverrideSchemaNameWithNamespaceAndVersionFromConfig() {
        final Map<String, String> config = new HashMap<>();
        config.put("schema.namespace", "foo.bar");
        config.put("schema.name", "baz");
        config.put("schema.version", "42");
        Schema schema = SchemaBuilder.struct().name("non.baz").version(0).build();

        final SinkRecord record = buildRecordAndApplyConfig(schema, config);

        assertEquals("foo.bar.baz", record.valueSchema().name());
        assertEquals(42, record.valueSchema().version());
    }

    @Test
    public void shouldFailOnVersionWithMissingSchema() {
        final SinkRecord record = new SinkRecord("", 0, null, null, null, 42, 0);
        assertThrows(DataException.class, () -> xform.apply(record), "Schema required for [updating schema metadata]");
    }

    @Test
    public void shouldNotFailOnEmptyRecord() {
        final SinkRecord record = new SinkRecord("", 0, null, null, null, null, 0);
        final SinkRecord updatedRecord = xform.apply(record);

        assertNull(updatedRecord.key());
        assertNull(updatedRecord.keySchema());
        assertNull(updatedRecord.value());
        assertNull(updatedRecord.valueSchema());
    }

    @Test
    public void schemaNameWithNamespaceAndVersionUpdateWithStruct() {
        final Schema schema = SchemaBuilder.struct()
                .name("my.origin.schema")
                .field("stringField", Schema.STRING_SCHEMA)
                .field("intField", Schema.INT32_SCHEMA)
                .build();
        final Struct value = new Struct(schema)
                .put("stringField", "value")
                .put("intField", 1);

        final Map<String, String> props = new HashMap<>();
        props.put("schema.namespace", "foo.bar");
        props.put("schema.name", "baz");
        props.put("schema.version", "42");
        xform.configure(props);

        final SinkRecord record = new SinkRecord("", 0, null, null, schema, value, 0);
        final SinkRecord updatedRecord = xform.apply(record);
        Struct newValue = (Struct) updatedRecord.value();

        assertEquals("foo.bar.baz", updatedRecord.valueSchema().name());
        assertEquals(42, updatedRecord.valueSchema().version());
        // Make sure the struct's schema and fields all point to the new schema
        assertMatchingSchema(newValue.schema(), updatedRecord.valueSchema());
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

        Struct newValue = (Struct) SetSchemaMetadata.updateSchemaIn(value, newSchema);
        assertMatchingSchema(newValue.schema(), newSchema);
    }

    @Test
    public void updateSchemaOfNonStruct() {
        Object value = 1;
        Object updatedValue = SetSchemaMetadata.updateSchemaIn(value, Schema.INT32_SCHEMA);
        assertSame(value, updatedValue);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void updateSchemaOfNull() {
        Object updatedValue = SetSchemaMetadata.updateSchemaIn(null, Schema.INT32_SCHEMA);
        assertNull(updatedValue);
    }

    protected void assertMatchingSchema(Schema actual, Schema expected) {
        assertSame(expected, actual);
        assertEquals(expected.name(), actual.name());
        for (Field field : expected.fields()) {
            String fieldName = field.name();
            assertEquals(expected.field(fieldName).name(), actual.field(fieldName).name());
            assertEquals(expected.field(fieldName).index(), actual.field(fieldName).index());
            assertSame(expected.field(fieldName).schema(), actual.field(fieldName).schema());
        }
    }

    private SinkRecord buildRecordAndApplyConfig(Schema recordSchema, Map<String, String> xformConfig) {
        xform.configure(xformConfig);
        final SinkRecord record = new SinkRecord("", 0, null, null, recordSchema, null, 0);
        return xform.apply(record);
    }
}
