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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CastTest {
    private final Cast<SourceRecord> xformKey = new Cast.Key<>();
    private final Cast<SourceRecord> xformValue = new Cast.Value<>();

    @After
    public void teardown() {
        xformKey.close();
        xformValue.close();
    }

    @Test(expected = ConfigException.class)
    public void testConfigEmpty() {
        xformKey.configure(Collections.singletonMap(Cast.SPEC_CONFIG, ""));
    }

    @Test(expected = ConfigException.class)
    public void testConfigInvalidSchemaType() {
        xformKey.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "foo:faketype"));
    }

    @Test(expected = ConfigException.class)
    public void testConfigInvalidTargetType() {
        xformKey.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "foo:array"));
    }

    @Test(expected = ConfigException.class)
    public void testConfigInvalidMap() {
        xformKey.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "foo:int8:extra"));
    }

    @Test(expected = ConfigException.class)
    public void testConfigMixWholeAndFieldTransformation() {
        xformKey.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "foo:int8,int32"));
    }

    @Test
    public void castWholeRecordKeyWithSchema() {
        xformKey.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int8"));
        SourceRecord transformed = xformKey.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42, Schema.STRING_SCHEMA, "bogus"));

        assertEquals(Schema.Type.INT8, transformed.keySchema().type());
        assertEquals((byte) 42, transformed.key());
    }

    @Test
    public void castWholeRecordValueWithSchemaInt8() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int8"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.INT8, transformed.valueSchema().type());
        assertEquals((byte) 42, transformed.value());
    }

    @Test
    public void castWholeRecordValueWithSchemaInt16() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int16"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.INT16, transformed.valueSchema().type());
        assertEquals((short) 42, transformed.value());
    }

    @Test
    public void castWholeRecordValueWithSchemaInt32() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int32"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.INT32, transformed.valueSchema().type());
        assertEquals(42, transformed.value());
    }

    @Test
    public void castWholeRecordValueWithSchemaInt64() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int64"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.INT64, transformed.valueSchema().type());
        assertEquals((long) 42, transformed.value());
    }

    @Test
    public void castWholeRecordValueWithSchemaFloat32() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "float32"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.FLOAT32, transformed.valueSchema().type());
        assertEquals(42.f, transformed.value());
    }

    @Test
    public void castWholeRecordValueWithSchemaFloat64() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "float64"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.FLOAT64, transformed.valueSchema().type());
        assertEquals(42., transformed.value());
    }

    @Test
    public void castWholeRecordValueWithSchemaBooleanTrue() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "boolean"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.BOOLEAN, transformed.valueSchema().type());
        assertEquals(true, transformed.value());
    }

    @Test
    public void castWholeRecordValueWithSchemaBooleanFalse() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "boolean"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 0));

        assertEquals(Schema.Type.BOOLEAN, transformed.valueSchema().type());
        assertEquals(false, transformed.value());
    }

    @Test
    public void castWholeRecordValueWithSchemaString() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "string"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.STRING, transformed.valueSchema().type());
        assertEquals("42", transformed.value());
    }

    @Test
    public void castWholeRecordDefaultValue() {
        // Validate default value in schema is correctly converted
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int32"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                SchemaBuilder.float32().defaultValue(-42.125f).build(), 42.125f));

        assertEquals(Schema.Type.INT32, transformed.valueSchema().type());
        assertEquals(42, transformed.value());
        assertEquals(-42, transformed.valueSchema().defaultValue());
    }

    @Test
    public void castWholeRecordKeySchemaless() {
        xformKey.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int8"));
        SourceRecord transformed = xformKey.apply(new SourceRecord(null, null, "topic", 0,
                null, 42, Schema.STRING_SCHEMA, "bogus"));

        assertNull(transformed.keySchema());
        assertEquals((byte) 42, transformed.key());
    }

    @Test
    public void castWholeRecordValueSchemalessInt8() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int8"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals((byte) 42, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessInt16() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int16"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals((short) 42, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessInt32() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int32"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals(42, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessInt64() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int64"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals((long) 42, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessFloat32() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "float32"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals(42.f, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessFloat64() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "float64"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals(42., transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessBooleanTrue() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "boolean"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals(true, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessBooleanFalse() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "boolean"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                null, 0));

        assertNull(transformed.valueSchema());
        assertEquals(false, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessString() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "string"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals("42", transformed.value());
    }

    @Test(expected = DataException.class)
    public void castWholeRecordValueSchemalessUnsupportedType() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int8"));
        xformValue.apply(new SourceRecord(null, null, "topic", 0, null, Collections.singletonList("foo")));
    }


    @Test
    public void castFieldsWithSchema() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int8:int16,int16:int32,int32:int64,int64:boolean,float32:float64,float64:boolean,boolean:int8,string:int32,optional:int32"));

        // Include an optional fields and fields with defaults to validate their values are passed through properly
        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("int8", Schema.INT8_SCHEMA);
        builder.field("int16", Schema.OPTIONAL_INT16_SCHEMA);
        builder.field("int32", SchemaBuilder.int32().defaultValue(2).build());
        builder.field("int64", Schema.INT64_SCHEMA);
        builder.field("float32", Schema.FLOAT32_SCHEMA);
        // Default value here ensures we correctly convert default values
        builder.field("float64", SchemaBuilder.float64().defaultValue(-1.125).build());
        builder.field("boolean", Schema.BOOLEAN_SCHEMA);
        builder.field("string", Schema.STRING_SCHEMA);
        builder.field("optional", Schema.OPTIONAL_FLOAT32_SCHEMA);
        Schema supportedTypesSchema = builder.build();

        Struct recordValue = new Struct(supportedTypesSchema);
        recordValue.put("int8", (byte) 8);
        recordValue.put("int16", (short) 16);
        recordValue.put("int32", 32);
        recordValue.put("int64", (long) 64);
        recordValue.put("float32", 32.f);
        recordValue.put("float64", -64.);
        recordValue.put("boolean", true);
        recordValue.put("string", "42");
        // optional field intentionally omitted

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                supportedTypesSchema, recordValue));

        assertEquals((short) 8, ((Struct) transformed.value()).get("int8"));
        assertTrue(((Struct) transformed.value()).schema().field("int16").schema().isOptional());
        assertEquals(16, ((Struct) transformed.value()).get("int16"));
        assertEquals((long) 32, ((Struct) transformed.value()).get("int32"));
        assertEquals(2L, ((Struct) transformed.value()).schema().field("int32").schema().defaultValue());
        assertEquals(true, ((Struct) transformed.value()).get("int64"));
        assertEquals(32., ((Struct) transformed.value()).get("float32"));
        assertEquals(true, ((Struct) transformed.value()).get("float64"));
        assertEquals(true, ((Struct) transformed.value()).schema().field("float64").schema().defaultValue());
        assertEquals((byte) 1, ((Struct) transformed.value()).get("boolean"));
        assertEquals(42, ((Struct) transformed.value()).get("string"));
        assertNull(((Struct) transformed.value()).get("optional"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void castFieldsSchemaless() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int8:int16,int16:int32,int32:int64,int64:boolean,float32:float64,float64:boolean,boolean:int8,string:int32"));
        Map<String, Object> recordValue = new HashMap<>();
        recordValue.put("int8", (byte) 8);
        recordValue.put("int16", (short) 16);
        recordValue.put("int32", 32);
        recordValue.put("int64", (long) 64);
        recordValue.put("float32", 32.f);
        recordValue.put("float64", -64.);
        recordValue.put("boolean", true);
        recordValue.put("string", "42");
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                null, recordValue));

        assertNull(transformed.valueSchema());
        assertEquals((short) 8, ((Map<String, Object>) transformed.value()).get("int8"));
        assertEquals(16, ((Map<String, Object>) transformed.value()).get("int16"));
        assertEquals((long) 32, ((Map<String, Object>) transformed.value()).get("int32"));
        assertEquals(true, ((Map<String, Object>) transformed.value()).get("int64"));
        assertEquals(32., ((Map<String, Object>) transformed.value()).get("float32"));
        assertEquals(true, ((Map<String, Object>) transformed.value()).get("float64"));
        assertEquals((byte) 1, ((Map<String, Object>) transformed.value()).get("boolean"));
        assertEquals(42, ((Map<String, Object>) transformed.value()).get("string"));
    }

}
