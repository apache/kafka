/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CastTest {

    @Test
    public void castWholeRecordKeySchemad() {
        final Cast<SourceRecord> xform = new Cast.Key<>();
        xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int8"));
        SourceRecord transformed = xform.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42, Schema.STRING_SCHEMA, "bogus"));

        assertEquals(Schema.Type.INT8, transformed.keySchema().type());
        assertEquals((byte) 42, transformed.key());
    }

    @Test
    public void castWholeRecordValueSchemadInt8() {
        final Cast<SourceRecord> xform = new Cast.Value<>();
        xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int8"));
        SourceRecord transformed = xform.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.INT8, transformed.valueSchema().type());
        assertEquals((byte) 42, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemadInt16() {
        final Cast<SourceRecord> xform = new Cast.Value<>();
        xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int16"));
        SourceRecord transformed = xform.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.INT16, transformed.valueSchema().type());
        assertEquals((short) 42, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemadInt32() {
        final Cast<SourceRecord> xform = new Cast.Value<>();
        xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int32"));
        SourceRecord transformed = xform.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.INT32, transformed.valueSchema().type());
        assertEquals(42, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemadInt64() {
        final Cast<SourceRecord> xform = new Cast.Value<>();
        xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int64"));
        SourceRecord transformed = xform.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.INT64, transformed.valueSchema().type());
        assertEquals((long) 42, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemadFloat32() {
        final Cast<SourceRecord> xform = new Cast.Value<>();
        xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "float32"));
        SourceRecord transformed = xform.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.FLOAT32, transformed.valueSchema().type());
        assertEquals(42.f, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemadFloat64() {
        final Cast<SourceRecord> xform = new Cast.Value<>();
        xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "float64"));
        SourceRecord transformed = xform.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.FLOAT64, transformed.valueSchema().type());
        assertEquals(42., transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemadBooleanTrue() {
        final Cast<SourceRecord> xform = new Cast.Value<>();
        xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "boolean"));
        SourceRecord transformed = xform.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.BOOLEAN, transformed.valueSchema().type());
        assertEquals(true, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemadBooleanFalse() {
        final Cast<SourceRecord> xform = new Cast.Value<>();
        xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "boolean"));
        SourceRecord transformed = xform.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 0));

        assertEquals(Schema.Type.BOOLEAN, transformed.valueSchema().type());
        assertEquals(false, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemadString() {
        final Cast<SourceRecord> xform = new Cast.Value<>();
        xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "string"));
        SourceRecord transformed = xform.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.STRING, transformed.valueSchema().type());
        assertEquals("42", transformed.value());
    }

    @Test
    public void castWholeRecordKeySchemaless() {
        final Cast<SourceRecord> xform = new Cast.Key<>();
        xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int8"));
        SourceRecord transformed = xform.apply(new SourceRecord(null, null, "topic", 0,
                null, 42, Schema.STRING_SCHEMA, "bogus"));

        assertNull(transformed.keySchema());
        assertEquals((byte) 42, transformed.key());
    }

    @Test
    public void castWholeRecordValueSchemalessInt8() {
        final Cast<SourceRecord> xform = new Cast.Value<>();
        xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int8"));
        SourceRecord transformed = xform.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals((byte) 42, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessInt16() {
        final Cast<SourceRecord> xform = new Cast.Value<>();
        xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int16"));
        SourceRecord transformed = xform.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals((short) 42, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessInt32() {
        final Cast<SourceRecord> xform = new Cast.Value<>();
        xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int32"));
        SourceRecord transformed = xform.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals(42, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessInt64() {
        final Cast<SourceRecord> xform = new Cast.Value<>();
        xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int64"));
        SourceRecord transformed = xform.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals((long) 42, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessFloat32() {
        final Cast<SourceRecord> xform = new Cast.Value<>();
        xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "float32"));
        SourceRecord transformed = xform.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals(42.f, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessFloat64() {
        final Cast<SourceRecord> xform = new Cast.Value<>();
        xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "float64"));
        SourceRecord transformed = xform.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals(42., transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessBooleanTrue() {
        final Cast<SourceRecord> xform = new Cast.Value<>();
        xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "boolean"));
        SourceRecord transformed = xform.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals(true, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessBooleanFalse() {
        final Cast<SourceRecord> xform = new Cast.Value<>();
        xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "boolean"));
        SourceRecord transformed = xform.apply(new SourceRecord(null, null, "topic", 0,
                null, 0));

        assertNull(transformed.valueSchema());
        assertEquals(false, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessString() {
        final Cast<SourceRecord> xform = new Cast.Value<>();
        xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "string"));
        SourceRecord transformed = xform.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals("42", transformed.value());
    }

    @Test
    public void castFieldsSchemad() {
        final Cast<SourceRecord> xform = new Cast.Value<>();
        xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int8:int16,int16:int32,int32:int64,int64:boolean,float32:float64,float64:boolean,boolean:int8,string:int32"));

        // Include an optional fields and fields with defaults to validate their values are passed through properly
        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("int8", Schema.INT8_SCHEMA);
        builder.field("int16", Schema.OPTIONAL_INT16_SCHEMA);
        builder.field("int32", SchemaBuilder.int32().defaultValue(2).build());
        builder.field("int64", Schema.INT64_SCHEMA);
        builder.field("float32", Schema.FLOAT32_SCHEMA);
        builder.field("float64", Schema.FLOAT64_SCHEMA);
        builder.field("boolean", Schema.BOOLEAN_SCHEMA);
        builder.field("string", Schema.STRING_SCHEMA);
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

        SourceRecord transformed = xform.apply(new SourceRecord(null, null, "topic", 0,
                supportedTypesSchema, recordValue));

        assertEquals((short) 8, ((Struct) transformed.value()).get("int8"));
        assertTrue(((Struct) transformed.value()).schema().field("int16").schema().isOptional());
        assertEquals(16, ((Struct) transformed.value()).get("int16"));
        assertEquals((long) 32, ((Struct) transformed.value()).get("int32"));
        assertEquals(2L, ((Struct) transformed.value()).schema().field("int32").schema().defaultValue());
        assertEquals(true, ((Struct) transformed.value()).get("int64"));
        assertEquals(32., ((Struct) transformed.value()).get("float32"));
        assertEquals(false, ((Struct) transformed.value()).get("float64"));
        assertEquals((byte) 1, ((Struct) transformed.value()).get("boolean"));
        assertEquals(42, ((Struct) transformed.value()).get("string"));
    }

    @Test
    public void castFieldsSchemaless() {
        final Cast<SourceRecord> xform = new Cast.Value<>();
        xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int8:int16,int16:int32,int32:int64,int64:boolean,float32:float64,float64:boolean,boolean:int8,string:int32"));
        Map<String, Object> recordValue = new HashMap<>();
        recordValue.put("int8", (byte) 8);
        recordValue.put("int16", (short) 16);
        recordValue.put("int32", 32);
        recordValue.put("int64", (long) 64);
        recordValue.put("float32", 32.f);
        recordValue.put("float64", -64.);
        recordValue.put("boolean", true);
        recordValue.put("string", "42");
        SourceRecord transformed = xform.apply(new SourceRecord(null, null, "topic", 0,
                null, recordValue));

        assertNull(transformed.valueSchema());
        assertEquals((short) 8, ((Map<String, Object>) transformed.value()).get("int8"));
        assertEquals(16, ((Map<String, Object>) transformed.value()).get("int16"));
        assertEquals((long) 32, ((Map<String, Object>) transformed.value()).get("int32"));
        assertEquals(true, ((Map<String, Object>) transformed.value()).get("int64"));
        assertEquals(32., ((Map<String, Object>) transformed.value()).get("float32"));
        assertEquals(false, ((Map<String, Object>) transformed.value()).get("float64"));
        assertEquals((byte) 1, ((Map<String, Object>) transformed.value()).get("boolean"));
        assertEquals(42, ((Map<String, Object>) transformed.value()).get("string"));
    }

}
