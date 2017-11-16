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
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class SetSchemaMetadataTest {
    private final SetSchemaMetadata<SinkRecord> xform = new SetSchemaMetadata.Value<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test
    public void schemaNameUpdate() {
        xform.configure(Collections.singletonMap("schema.name", "foo"));
        final SinkRecord record = new SinkRecord("", 0, null, null, SchemaBuilder.struct().build(), null, 0);
        final SinkRecord updatedRecord = xform.apply(record);
        assertEquals("foo", updatedRecord.valueSchema().name());
    }

    @Test
    public void schemaVersionUpdate() {
        xform.configure(Collections.singletonMap("schema.version", 42));
        final SinkRecord record = new SinkRecord("", 0, null, null, SchemaBuilder.struct().build(), null, 0);
        final SinkRecord updatedRecord = xform.apply(record);
        assertEquals(new Integer(42), updatedRecord.valueSchema().version());
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
        assertEquals(new Integer(42), updatedRecord.valueSchema().version());
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
        assertEquals(new Integer(42), updatedRecord.valueSchema().version());

        // Make sure the struct's schema and fields all point to the new schema
        assertMatchingSchema((Struct) updatedRecord.value(), updatedRecord.valueSchema());
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
        assertMatchingSchema(newValue, newSchema);
    }

    @Test
    public void updateSchemaOfNonStruct() {
        Object value = new Integer(1);
        Object updatedValue = SetSchemaMetadata.updateSchemaIn(value, Schema.INT32_SCHEMA);
        assertSame(value, updatedValue);
    }

    @Test
    public void updateSchemaOfNull() {
        Object updatedValue = SetSchemaMetadata.updateSchemaIn(null, Schema.INT32_SCHEMA);
        assertEquals(null, updatedValue);
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
