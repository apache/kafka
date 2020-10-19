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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CastTest {
    private final Cast<SourceRecord> xformKey = new Cast.Key<>();
    private final Cast<SourceRecord> xformValue = new Cast.Value<>();
    private static final long MILLIS_PER_HOUR = TimeUnit.HOURS.toMillis(1);
    private static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1);

    @After
    public void teardown() {
        xformKey.close();
        xformValue.close();
    }

    @Test(expected = ConfigException.class)
    public void testConfigEmpty() {
        xformKey.configure(Collections.singletonMap(Cast.ConfigName.SPEC, ""));
    }

    @Test(expected = ConfigException.class)
    public void testConfigInvalidSchemaType() {
        xformKey.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "foo:faketype"));
    }

    @Test(expected = ConfigException.class)
    public void testConfigInvalidTargetType() {
        xformKey.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "foo:array"));
    }

    @Test(expected = ConfigException.class)
    public void testUnsupportedTargetType() {
        xformKey.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "foo:bytes"));
    }

    @Test(expected = ConfigException.class)
    public void testConfigInvalidMap() {
        xformKey.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "foo:int8:extra"));
    }

    @Test(expected = ConfigException.class)
    public void testConfigMixWholeAndFieldTransformation() {
        xformKey.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "foo:int8,int32"));
    }

    @Test
    public void castWholeRecordKeyWithSchema() {
        xformKey.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "int8"));
        SourceRecord transformed = xformKey.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42, Schema.STRING_SCHEMA, "bogus"));

        assertEquals(Schema.Type.INT8, transformed.keySchema().type());
        assertEquals((byte) 42, transformed.key());
    }

    @Test
    public void castWholeRecordValueWithSchemaInt8() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "int8"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.INT8, transformed.valueSchema().type());
        assertEquals((byte) 42, transformed.value());
    }

    @Test
    public void castWholeRecordValueWithSchemaInt16() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "int16"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.INT16, transformed.valueSchema().type());
        assertEquals((short) 42, transformed.value());
    }

    @Test
    public void castWholeRecordValueWithSchemaInt32() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "int32"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.INT32, transformed.valueSchema().type());
        assertEquals(42, transformed.value());
    }

    @Test
    public void castWholeRecordValueWithSchemaInt64() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "int64"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.INT64, transformed.valueSchema().type());
        assertEquals((long) 42, transformed.value());
    }

    @Test
    public void castWholeRecordValueWithSchemaFloat32() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "float32"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.FLOAT32, transformed.valueSchema().type());
        assertEquals(42.f, transformed.value());
    }

    @Test
    public void castWholeRecordValueWithSchemaFloat64() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "float64"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.FLOAT64, transformed.valueSchema().type());
        assertEquals(42., transformed.value());
    }

    @Test
    public void castWholeRecordValueWithSchemaBooleanTrue() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "boolean"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.BOOLEAN, transformed.valueSchema().type());
        assertEquals(true, transformed.value());
    }

    @Test
    public void castWholeRecordValueWithSchemaBooleanFalse() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "boolean"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 0));

        assertEquals(Schema.Type.BOOLEAN, transformed.valueSchema().type());
        assertEquals(false, transformed.value());
    }

    @Test
    public void castWholeRecordValueWithSchemaString() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "string"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                Schema.INT32_SCHEMA, 42));

        assertEquals(Schema.Type.STRING, transformed.valueSchema().type());
        assertEquals("42", transformed.value());
    }

    @Test
    public void castWholeBigDecimalRecordValueWithSchemaString() {
        BigDecimal bigDecimal = new BigDecimal(42);
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "string"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                Decimal.schema(bigDecimal.scale()), bigDecimal));

        assertEquals(Schema.Type.STRING, transformed.valueSchema().type());
        assertEquals("42", transformed.value());
    }

    @Test
    public void castWholeDateRecordValueWithSchemaString() {
        Date timestamp = new Date(MILLIS_PER_DAY + 1); // day + 1msec to get a timestamp formatting.
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "string"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                Timestamp.SCHEMA, timestamp));

        assertEquals(Schema.Type.STRING, transformed.valueSchema().type());
        assertEquals(Values.dateFormatFor(timestamp).format(timestamp), transformed.value());
    }

    @Test
    public void castWholeRecordDefaultValue() {
        // Validate default value in schema is correctly converted
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "int32"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                SchemaBuilder.float32().defaultValue(-42.125f).build(), 42.125f));

        assertEquals(Schema.Type.INT32, transformed.valueSchema().type());
        assertEquals(42, transformed.value());
        assertEquals(-42, transformed.valueSchema().defaultValue());
    }

    @Test
    public void castWholeRecordKeySchemaless() {
        xformKey.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "int8"));
        SourceRecord transformed = xformKey.apply(new SourceRecord(null, null, "topic", 0,
                null, 42, Schema.STRING_SCHEMA, "bogus"));

        assertNull(transformed.keySchema());
        assertEquals((byte) 42, transformed.key());
    }

    @Test
    public void castWholeRecordValueSchemalessInt8() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "int8"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals((byte) 42, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessInt16() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "int16"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals((short) 42, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessInt32() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "int32"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals(42, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessInt64() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "int64"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals((long) 42, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessFloat32() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "float32"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals(42.f, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessFloat64() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "float64"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals(42., transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessBooleanTrue() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "boolean"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals(true, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessBooleanFalse() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "boolean"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                null, 0));

        assertNull(transformed.valueSchema());
        assertEquals(false, transformed.value());
    }

    @Test
    public void castWholeRecordValueSchemalessString() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "string"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                null, 42));

        assertNull(transformed.valueSchema());
        assertEquals("42", transformed.value());
    }

    @Test(expected = DataException.class)
    public void castWholeRecordValueSchemalessUnsupportedType() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "int8"));
        xformValue.apply(new SourceRecord(null, null, "topic", 0, null, Collections.singletonList("foo")));
    }

    @Test
    public void castLogicalToPrimitive() {
        List<String> specParts = Arrays.asList(
            "date_to_int32:int32",  // Cast to underlying representation
            "timestamp_to_int64:int64",  // Cast to underlying representation
            "time_to_int64:int64",  // Cast to wider datatype than underlying representation
            "decimal_to_int32:int32",  // Cast to narrower datatype with data loss
            "timestamp_to_float64:float64",  // loss of precision casting to double
            "null_timestamp_to_int32:int32"
        );

        Date day = new Date(MILLIS_PER_DAY);
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC,
            String.join(",", specParts)));

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("date_to_int32", org.apache.kafka.connect.data.Date.SCHEMA);
        builder.field("timestamp_to_int64", Timestamp.SCHEMA);
        builder.field("time_to_int64", Time.SCHEMA);
        builder.field("decimal_to_int32", Decimal.schema(new BigDecimal((long) Integer.MAX_VALUE + 1).scale()));
        builder.field("timestamp_to_float64", Timestamp.SCHEMA);
        builder.field("null_timestamp_to_int32", Timestamp.builder().optional().build());

        Schema supportedTypesSchema = builder.build();

        Struct recordValue = new Struct(supportedTypesSchema);
        recordValue.put("date_to_int32", day);
        recordValue.put("timestamp_to_int64", new Date(0));
        recordValue.put("time_to_int64", new Date(1));
        recordValue.put("decimal_to_int32", new BigDecimal((long) Integer.MAX_VALUE + 1));
        recordValue.put("timestamp_to_float64", new Date(Long.MAX_VALUE));
        recordValue.put("null_timestamp_to_int32", null);

        SourceRecord transformed = xformValue.apply(
            new SourceRecord(null, null, "topic", 0,
                supportedTypesSchema, recordValue));

        assertEquals(1, ((Struct) transformed.value()).get("date_to_int32"));
        assertEquals(0L, ((Struct) transformed.value()).get("timestamp_to_int64"));
        assertEquals(1L, ((Struct) transformed.value()).get("time_to_int64"));
        assertEquals(Integer.MIN_VALUE, ((Struct) transformed.value()).get("decimal_to_int32"));
        assertEquals(9.223372036854776E18, ((Struct) transformed.value()).get("timestamp_to_float64"));
        assertNull(((Struct) transformed.value()).get("null_timestamp_to_int32"));

        Schema transformedSchema = ((Struct) transformed.value()).schema();
        assertEquals(Type.INT32, transformedSchema.field("date_to_int32").schema().type());
        assertEquals(Type.INT64, transformedSchema.field("timestamp_to_int64").schema().type());
        assertEquals(Type.INT64, transformedSchema.field("time_to_int64").schema().type());
        assertEquals(Type.INT32, transformedSchema.field("decimal_to_int32").schema().type());
        assertEquals(Type.FLOAT64, transformedSchema.field("timestamp_to_float64").schema().type());
        assertEquals(Type.INT32, transformedSchema.field("null_timestamp_to_int32").schema().type());
    }

    @Test
    public void castLogicalToString() {
        Date date = new Date(MILLIS_PER_DAY);
        Date time = new Date(MILLIS_PER_HOUR);
        Date timestamp = new Date();

        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC,
            "date:string,decimal:string,time:string,timestamp:string"));

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("date", org.apache.kafka.connect.data.Date.SCHEMA);
        builder.field("decimal", Decimal.schema(new BigDecimal(1982).scale()));
        builder.field("time", Time.SCHEMA);
        builder.field("timestamp", Timestamp.SCHEMA);

        Schema supportedTypesSchema = builder.build();

        Struct recordValue = new Struct(supportedTypesSchema);
        recordValue.put("date", date);
        recordValue.put("decimal", new BigDecimal(1982));
        recordValue.put("time", time);
        recordValue.put("timestamp", timestamp);

        SourceRecord transformed = xformValue.apply(
            new SourceRecord(null, null, "topic", 0,
                supportedTypesSchema, recordValue));

        assertEquals(Values.dateFormatFor(date).format(date), ((Struct) transformed.value()).get("date"));
        assertEquals("1982", ((Struct) transformed.value()).get("decimal"));
        assertEquals(Values.dateFormatFor(time).format(time), ((Struct) transformed.value()).get("time"));
        assertEquals(Values.dateFormatFor(timestamp).format(timestamp), ((Struct) transformed.value()).get("timestamp"));

        Schema transformedSchema = ((Struct) transformed.value()).schema();
        assertEquals(Type.STRING, transformedSchema.field("date").schema().type());
        assertEquals(Type.STRING, transformedSchema.field("decimal").schema().type());
        assertEquals(Type.STRING, transformedSchema.field("time").schema().type());
        assertEquals(Type.STRING, transformedSchema.field("timestamp").schema().type());
    }

    @Test
    public void castArrayToString() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC,
            "array:string"));

        Schema arraySchema = SchemaBuilder.array(Schema.INT8_SCHEMA).build();
        Schema supportedTypesSchema = SchemaBuilder.struct()
        		.field("array", arraySchema)
        		.build();

        List<Byte> array = Arrays.asList((byte) 1, (byte) 2);

        Struct recordValue = new Struct(supportedTypesSchema);
        recordValue.put("array", array);

        SourceRecord transformed = xformValue.apply(
                new SourceRecord(null, null, "topic", 0,
                        supportedTypesSchema, recordValue));

        //Compare simple string "[1,2]" against result after removing any spaces (just in case of variations in deepToString)
        assertEquals("[1,2]", ((Struct) transformed.value()).get("array").toString().replaceAll("\\s+",""));

        Schema transformedSchema = ((Struct) transformed.value()).schema();
        assertEquals(Schema.STRING_SCHEMA.type(), transformedSchema.field("array").schema().type());
    }

    @Test
    public void castComplexArrayToString() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC,
            "array:string"));

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("name", Schema.STRING_SCHEMA);
        builder.field("value", Schema.INT32_SCHEMA);
        builder.field("boolean", Schema.BOOLEAN_SCHEMA);
        Schema arrayMemberSchema = builder.build();

        Schema arraySchema = SchemaBuilder.array(arrayMemberSchema).build();

        builder = SchemaBuilder.struct();
        builder.field("array", arraySchema);
        Schema supportedTypesSchema = builder.build();

        Struct arrayMember1 = new Struct(arrayMemberSchema);
        arrayMember1.put("name", "Member 1");
        arrayMember1.put("value", 15);
        arrayMember1.put("boolean", true);

        Struct arrayMember2 = new Struct(arrayMemberSchema);
        arrayMember2.put("name", "Member 2");
        arrayMember2.put("value", 800);
        arrayMember2.put("boolean", false);

        List<Struct> array = new ArrayList<Struct>();
        array.add(arrayMember1);
        array.add(arrayMember2);

        Struct recordValue = new Struct(supportedTypesSchema);
        recordValue.put("array", array);

        SourceRecord transformed = xformValue.apply(
                new SourceRecord(null, null, "topic", 0,
                        supportedTypesSchema, recordValue));

        //Compare simple string against result after removing any spaces (just in case of variations in deepToString)
        assertEquals("[" + arrayMember1.toString().replaceAll("\\s+","") + "," + arrayMember2.toString().replaceAll("\\s+","") + "]",
        		((Struct) transformed.value()).get("array").toString().replaceAll("\\s+",""));

        Schema transformedSchema = ((Struct) transformed.value()).schema();
        assertEquals(Schema.STRING_SCHEMA.type(), transformedSchema.field("array").schema().type());
    }

    @Test
    public void castComplexArrayToJsonString() {
        Map<String, Object> config = new HashMap<>();
        config.put(Cast.ConfigName.SPEC, "array:string");
        config.put(Cast.ConfigName.COMPLEX_STRING_AS_JSON, true);
        xformValue.configure(config);

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("name", Schema.STRING_SCHEMA);
        builder.field("value", Schema.INT32_SCHEMA);
        builder.field("boolean", Schema.BOOLEAN_SCHEMA);
        Schema arrayMemberSchema = builder.build();

        Schema arraySchema = SchemaBuilder.array(arrayMemberSchema).build();

        builder = SchemaBuilder.struct();
        builder.field("array", arraySchema);
        Schema supportedTypesSchema = builder.build();

        Struct arrayMember1 = new Struct(arrayMemberSchema);
        arrayMember1.put("name", "Member 1");
        arrayMember1.put("value", 15);
        arrayMember1.put("boolean", true);

        Struct arrayMember2 = new Struct(arrayMemberSchema);
        arrayMember2.put("name", "Member 2");
        arrayMember2.put("value", 800);
        arrayMember2.put("boolean", false);

        List<Struct> array = new ArrayList<Struct>();
        array.add(arrayMember1);
        array.add(arrayMember2);

        Struct recordValue = new Struct(supportedTypesSchema);
        recordValue.put("array", array);

        SourceRecord transformed = xformValue.apply(
                new SourceRecord(null, null, "topic", 0,
                        supportedTypesSchema, recordValue));

        //Compare simple string against result after removing any spaces (just in case of variations in deepToString)
        assertEquals("[{\"name\":\"Member 1\",\"value\":15,\"boolean\":true},{\"name\":\"Member 2\",\"value\":800,\"boolean\":false}]",
        		((Struct) transformed.value()).get("array").toString());

        Schema transformedSchema = ((Struct) transformed.value()).schema();
        assertEquals(Schema.STRING_SCHEMA.type(), transformedSchema.field("array").schema().type());
    }

    @Test
    public void castMapToString() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC,
            "map:string"));

        Schema mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();
        Schema supportedTypesSchema = SchemaBuilder.struct()
        		.field("map", mapSchema)
        		.build();

        Map<String, Object> map = new LinkedHashMap<String, Object>();
        map.put("key1", 1);
        map.put("key2", 2);

        Struct recordValue = new Struct(supportedTypesSchema);
        recordValue.put("map", map);

        SourceRecord transformed = xformValue.apply(
                new SourceRecord(null, null, "topic", 0,
                        supportedTypesSchema, recordValue));

        //Compare map.toString() with result
        assertEquals(map.toString(), ((Struct) transformed.value()).get("map").toString());

        Schema transformedSchema = ((Struct) transformed.value()).schema();
        assertEquals(Schema.STRING_SCHEMA.type(), transformedSchema.field("map").schema().type());
    }

    @Test
    public void castComplexMapToString() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC,
            "map:string"));

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("name", Schema.STRING_SCHEMA);
        builder.field("value", Schema.INT32_SCHEMA);
        builder.field("boolean", Schema.BOOLEAN_SCHEMA);
        Schema mapEntrySchema = builder.build();

        Schema mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, mapEntrySchema).build();

        builder = SchemaBuilder.struct();
        builder.field("map", mapSchema);
        Schema supportedTypesSchema = builder.build();

        Struct mapEntry1 = new Struct(mapEntrySchema);
        mapEntry1.put("name", "Member 1");
        mapEntry1.put("value", 15);
        mapEntry1.put("boolean", true);

        Struct mapEntry2 = new Struct(mapEntrySchema);
        mapEntry2.put("name", "Member 2");
        mapEntry2.put("value", 800);
        mapEntry2.put("boolean", false);

        Map<String, Object> map = new LinkedHashMap<String, Object>();
        map.put("key1", mapEntry1);
        map.put("key2", mapEntry2);

        Struct recordValue = new Struct(supportedTypesSchema);
        recordValue.put("map", map);

        SourceRecord transformed = xformValue.apply(
                new SourceRecord(null, null, "topic", 0,
                        supportedTypesSchema, recordValue));

        //Compare map.toString() with result
        assertEquals(map.toString(), ((Struct) transformed.value()).get("map").toString());

        Schema transformedSchema = ((Struct) transformed.value()).schema();
        assertEquals(Schema.STRING_SCHEMA.type(), transformedSchema.field("map").schema().type());
    }

    @Test
    public void castComplexMapToJsonString() {
        Map<String, Object> config = new HashMap<>();
        config.put(Cast.ConfigName.SPEC, "map:string");
        config.put(Cast.ConfigName.COMPLEX_STRING_AS_JSON, true);
        xformValue.configure(config);

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("name", Schema.STRING_SCHEMA);
        builder.field("value", Schema.INT32_SCHEMA);
        builder.field("boolean", Schema.BOOLEAN_SCHEMA);
        Schema mapEntrySchema = builder.build();

        Schema mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, mapEntrySchema).build();

        builder = SchemaBuilder.struct();
        builder.field("map", mapSchema);
        Schema supportedTypesSchema = builder.build();

        Struct mapEntry1 = new Struct(mapEntrySchema);
        mapEntry1.put("name", "Member 1");
        mapEntry1.put("value", 15);
        mapEntry1.put("boolean", true);

        Struct mapEntry2 = new Struct(mapEntrySchema);
        mapEntry2.put("name", "Member 2");
        mapEntry2.put("value", 800);
        mapEntry2.put("boolean", false);

        Map<String, Object> map = new LinkedHashMap<String, Object>();
        map.put("key1", mapEntry1);
        map.put("key2", mapEntry2);

        Struct recordValue = new Struct(supportedTypesSchema);
        recordValue.put("map", map);

        SourceRecord transformed = xformValue.apply(
                new SourceRecord(null, null, "topic", 0,
                        supportedTypesSchema, recordValue));

        //Compare JSON string of map with result
        assertEquals("{\"key1\":{\"name\":\"Member 1\",\"value\":15,\"boolean\":true},\"key2\":{\"name\":\"Member 2\",\"value\":800,\"boolean\":false}}", 
        		((Struct) transformed.value()).get("map").toString());

        Schema transformedSchema = ((Struct) transformed.value()).schema();
        assertEquals(Schema.STRING_SCHEMA.type(), transformedSchema.field("map").schema().type());
    }

    @Test
    public void castFieldsWithSchema() {
        Date day = new Date(MILLIS_PER_DAY);
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "int8:int16,int16:int32,int32:int64,int64:boolean,float32:float64,float64:boolean,boolean:int8,string:int32,bigdecimal:string,date:string,optional:int32"));

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
        builder.field("bigdecimal", Decimal.schema(new BigDecimal(42).scale()));
        builder.field("date", org.apache.kafka.connect.data.Date.SCHEMA);
        builder.field("optional", Schema.OPTIONAL_FLOAT32_SCHEMA);
        builder.field("timestamp", Timestamp.SCHEMA);
        Schema supportedTypesSchema = builder.build();

        Struct recordValue = new Struct(supportedTypesSchema);
        recordValue.put("int8", (byte) 8);
        recordValue.put("int16", (short) 16);
        recordValue.put("int32", 32);
        recordValue.put("int64", (long) 64);
        recordValue.put("float32", 32.f);
        recordValue.put("float64", -64.);
        recordValue.put("boolean", true);
        recordValue.put("bigdecimal", new BigDecimal(42));
        recordValue.put("date", day);
        recordValue.put("string", "42");
        recordValue.put("timestamp", new Date(0));
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
        assertEquals("42", ((Struct) transformed.value()).get("bigdecimal"));
        assertEquals(Values.dateFormatFor(day).format(day), ((Struct) transformed.value()).get("date"));
        assertEquals(new Date(0), ((Struct) transformed.value()).get("timestamp"));
        assertNull(((Struct) transformed.value()).get("optional"));

        Schema transformedSchema = ((Struct) transformed.value()).schema();
        assertEquals(Schema.INT16_SCHEMA.type(), transformedSchema.field("int8").schema().type());
        assertEquals(Schema.OPTIONAL_INT32_SCHEMA.type(), transformedSchema.field("int16").schema().type());
        assertEquals(Schema.INT64_SCHEMA.type(), transformedSchema.field("int32").schema().type());
        assertEquals(Schema.BOOLEAN_SCHEMA.type(), transformedSchema.field("int64").schema().type());
        assertEquals(Schema.FLOAT64_SCHEMA.type(), transformedSchema.field("float32").schema().type());
        assertEquals(Schema.BOOLEAN_SCHEMA.type(), transformedSchema.field("float64").schema().type());
        assertEquals(Schema.INT8_SCHEMA.type(), transformedSchema.field("boolean").schema().type());
        assertEquals(Schema.INT32_SCHEMA.type(), transformedSchema.field("string").schema().type());
        assertEquals(Schema.STRING_SCHEMA.type(), transformedSchema.field("bigdecimal").schema().type());
        assertEquals(Schema.STRING_SCHEMA.type(), transformedSchema.field("date").schema().type());
        assertEquals(Schema.OPTIONAL_INT32_SCHEMA.type(), transformedSchema.field("optional").schema().type());
        // The following fields are not changed
        assertEquals(Timestamp.SCHEMA.type(), transformedSchema.field("timestamp").schema().type());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void castFieldsSchemaless() {
        xformValue.configure(Collections.singletonMap(Cast.ConfigName.SPEC, "int8:int16,int16:int32,int32:int64,int64:boolean,float32:float64,float64:boolean,boolean:int8,string:int32"));
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

    @SuppressWarnings("unchecked")
	@Test
	public void castFieldsWithSchemaRecursive() {
	    Map<String, Object> config = new HashMap<>();
	    config.put(Cast.ConfigName.SPEC, "int8:int16,int32:int64,float64:boolean,boolean:int8,string:int32,bigdecimal:string,optional:int32,arraystring:string,mapstring:string,structstring:string");
	    config.put(Cast.ConfigName.RECURSIVE, true);
	    xformValue.configure(config);

	    // Schema

	    final Schema schema = SchemaBuilder.struct()
	    	    .field("int8", Schema.INT8_SCHEMA)
	    	    .field("int32", SchemaBuilder.int32().defaultValue(2).build())
	    	    // Default value here ensures we correctly convert default values
	    	    .field("float64", SchemaBuilder.float64().defaultValue(-1.125).build())
	    	    .field("boolean", Schema.BOOLEAN_SCHEMA)
	    	    .field("string", Schema.STRING_SCHEMA)
	    	    .field("bigdecimal", Decimal.schema(new BigDecimal(42).scale()))
	    	    .field("optional", Schema.OPTIONAL_FLOAT32_SCHEMA)
	            .build();

	    final Schema arraySchema = SchemaBuilder.array(schema);

	    final Schema parentASchema = SchemaBuilder.struct()
	    	    .field("string", Schema.STRING_SCHEMA)
	            .field("array", arraySchema)
	            .field("arraystring", arraySchema)
	    	    .field("struct", schema)
	    	    .field("structstring", schema)
	            .build();

	    final Schema mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, parentASchema);

	    final Schema parentBSchema = SchemaBuilder.struct()
	            .field("array", arraySchema)
	            .field("arraystring", arraySchema)
	    	    .field("struct", parentASchema)
	    	    .field("structstring", parentASchema)
	    	    .field("map", mapSchema)
	    	    .field("mapstring", mapSchema)
	            .build();

	    // Value

	    final Struct struct1 = new Struct(schema)
	    	    .put("int8", (byte) 8)
	    	    .put("int32", 32)
	    	    .put("float64", -64.)
	    	    .put("boolean", true)
	    	    .put("string", "42")
	    	    .put("bigdecimal", new BigDecimal(42));
        // optional field intentionally omitted

	    final Struct struct2 = new Struct(schema)
	    	    .put("int8", (byte) 8)
	    	    .put("int32", 32)
	    	    .put("float64", -64.)
	    	    .put("boolean", true)
	    	    .put("string", "42")
	    	    .put("bigdecimal", new BigDecimal(42));
        // optional field intentionally omitted

        final List<Object> array = new ArrayList<Object>();
        array.add(struct1);
        array.add(struct2);

        final Struct parentAValue = new Struct(parentASchema)
	    	    .put("string", "42")
	    	    .put("array", array)
	    	    .put("arraystring", array)
			    .put("struct", struct1)
			    .put("structstring", struct2);

        final Map<String, Object> map = new HashMap<>();
		map.put("key1", parentAValue);
		map.put("key2", parentAValue);

        final Struct parentBValue = new Struct(parentBSchema)
	    	    .put("array", array)
	    	    .put("arraystring", array)
			    .put("struct", parentAValue)
			    .put("structstring", parentAValue)
			    .put("map", map)
			    .put("mapstring", map);

        // Create and transform record

	    SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
	    		parentBSchema, parentBValue));

	    // Assert results

        final Struct updatedB = (Struct) transformed.value();
        assertEquals(6, updatedB.schema().fields().size());

        final List<Object> updatedB_array = (List<Object>) updatedB.get("array");
        assertEquals(2, updatedB_array.size());

        final Struct updatedB_array_0 = (Struct) updatedB_array.get(0);
	    assertEquals((short) 8, updatedB_array_0.get("int8"));
	    assertEquals((long) 32, updatedB_array_0.get("int32"));
	    assertEquals(2L, updatedB_array_0.schema().field("int32").schema().defaultValue());
	    assertEquals(true, updatedB_array_0.get("float64"));
	    assertEquals(true, updatedB_array_0.schema().field("float64").schema().defaultValue());
	    assertEquals((byte) 1, updatedB_array_0.get("boolean"));
	    assertEquals(42, updatedB_array_0.get("string"));
	    assertEquals("42", updatedB_array_0.get("bigdecimal"));
	    assertNull(updatedB_array_0.get("optional"));

        final Struct updatedB_array_1 = (Struct) updatedB_array.get(1);
	    assertEquals((short) 8, updatedB_array_1.get("int8"));
	    assertEquals((long) 32, updatedB_array_1.get("int32"));
	    assertEquals(2L, updatedB_array_1.schema().field("int32").schema().defaultValue());
	    assertEquals(true, updatedB_array_1.get("float64"));
	    assertEquals(true, updatedB_array_1.schema().field("float64").schema().defaultValue());
	    assertEquals((byte) 1, updatedB_array_1.get("boolean"));
	    assertEquals(42, updatedB_array_1.get("string"));
	    assertEquals("42", updatedB_array_1.get("bigdecimal"));
	    assertNull(updatedB_array_1.get("optional"));

	    // arraystring should match toString() of array
        assertEquals(array.toString(), updatedB.get("arraystring"));

        // B->struct should be of type A and have child fields converted as configured
        // B->A should have string, array, arraystring, struct, structstring
        final Struct updatedB_updatedA = (Struct) updatedB.get("struct");
	    assertEquals(42, updatedB_updatedA.get("string"));

	    // B->A->array should have 2 items converted from struct1 and struct2
        final List<Object> updatedB_updatedA_array = (List<Object>) updatedB_updatedA.get("array");
        assertEquals(2, updatedB_updatedA_array.size());

        final Struct updatedB_updatedA_array_0 = (Struct) updatedB_updatedA_array.get(0);
	    assertEquals((short) 8, updatedB_updatedA_array_0.get("int8"));
	    assertEquals((long) 32, updatedB_updatedA_array_0.get("int32"));
	    assertEquals(2L, updatedB_updatedA_array_0.schema().field("int32").schema().defaultValue());
	    assertEquals(true, updatedB_updatedA_array_0.get("float64"));
	    assertEquals(true, updatedB_updatedA_array_0.schema().field("float64").schema().defaultValue());
	    assertEquals((byte) 1, updatedB_updatedA_array_0.get("boolean"));
	    assertEquals(42, updatedB_updatedA_array_0.get("string"));
	    assertEquals("42", updatedB_updatedA_array_0.get("bigdecimal"));
	    assertNull(updatedB_updatedA_array_0.get("optional"));

        final Struct updatedB_updatedA_array_1 = (Struct) updatedB_updatedA_array.get(1);
	    assertEquals((short) 8, updatedB_updatedA_array_1.get("int8"));
	    assertEquals((long) 32, updatedB_updatedA_array_1.get("int32"));
	    assertEquals(2L, updatedB_updatedA_array_1.schema().field("int32").schema().defaultValue());
	    assertEquals(true, updatedB_updatedA_array_1.get("float64"));
	    assertEquals(true, updatedB_updatedA_array_1.schema().field("float64").schema().defaultValue());
	    assertEquals((byte) 1, updatedB_updatedA_array_1.get("boolean"));
	    assertEquals(42, updatedB_updatedA_array_1.get("string"));
	    assertEquals("42", updatedB_updatedA_array_1.get("bigdecimal"));
	    assertNull(updatedB_updatedA_array_1.get("optional"));

	    // B->A->arraystring should match toString() of array
        assertEquals(array.toString(), updatedB_updatedA.get("arraystring"));

	    // B->A->struct should be a converted version of struct1
	    final Struct updatedB_updatedA_struct = (Struct) updatedB_updatedA.get("struct");
	    assertEquals((short) 8, updatedB_updatedA_struct.get("int8"));
	    assertEquals((long) 32, updatedB_updatedA_struct.get("int32"));
	    assertEquals(2L, updatedB_updatedA_struct.schema().field("int32").schema().defaultValue());
	    assertEquals(true, updatedB_updatedA_struct.get("float64"));
	    assertEquals(true, updatedB_updatedA_struct.schema().field("float64").schema().defaultValue());
	    assertEquals((byte) 1, updatedB_updatedA_struct.get("boolean"));
	    assertEquals(42, updatedB_updatedA_struct.get("string"));
	    assertEquals("42", updatedB_updatedA_struct.get("bigdecimal"));
	    assertNull(updatedB_updatedA_struct.get("optional"));

        // B->A->structstring should match toString() of struct1
        assertEquals(struct2.toString(), updatedB_updatedA.get("structstring"));

        // B->structstring should match toString() of parentAValue
        assertEquals(parentAValue.toString(), updatedB.get("structstring"));

        // B->map should have two entries, both of which are converted structs of type A
        final Map<String, Object> updatedB_map = (Map<String, Object>) updatedB.get("map");
        assertEquals(2, updatedB_map.size());

        // Assert the entire structure and values of key1
        final Struct updatedB_map_key1 = (Struct) updatedB_map.get("key1");
	    assertEquals(42, updatedB_map_key1.get("string"));

	    // key1 array should have 2 items converted from struct1 and struct2
        final List<Object> updatedB_map_key1_array = (List<Object>) updatedB_map_key1.get("array");
        assertEquals(2, updatedB_map_key1_array.size());

        final Struct updatedB_map_key1_array_0 = (Struct) updatedB_map_key1_array.get(0);
	    assertEquals((short) 8, updatedB_map_key1_array_0.get("int8"));
	    assertEquals((long) 32, updatedB_map_key1_array_0.get("int32"));
	    assertEquals(2L, updatedB_map_key1_array_0.schema().field("int32").schema().defaultValue());
	    assertEquals(true, updatedB_map_key1_array_0.get("float64"));
	    assertEquals(true, updatedB_map_key1_array_0.schema().field("float64").schema().defaultValue());
	    assertEquals((byte) 1, updatedB_map_key1_array_0.get("boolean"));
	    assertEquals(42, updatedB_map_key1_array_0.get("string"));
	    assertEquals("42", updatedB_map_key1_array_0.get("bigdecimal"));
	    assertNull(updatedB_map_key1_array_0.get("optional"));

        final Struct updatedB_map_key1_array_1 = (Struct) updatedB_map_key1_array.get(1);
	    assertEquals((short) 8, updatedB_map_key1_array_1.get("int8"));
	    assertEquals((long) 32, updatedB_map_key1_array_1.get("int32"));
	    assertEquals(2L, updatedB_map_key1_array_1.schema().field("int32").schema().defaultValue());
	    assertEquals(true, updatedB_map_key1_array_1.get("float64"));
	    assertEquals(true, updatedB_map_key1_array_1.schema().field("float64").schema().defaultValue());
	    assertEquals((byte) 1, updatedB_map_key1_array_1.get("boolean"));
	    assertEquals(42, updatedB_map_key1_array_1.get("string"));
	    assertEquals("42", updatedB_map_key1_array_1.get("bigdecimal"));
	    assertNull(updatedB_map_key1_array_1.get("optional"));

	    // key1 arraystring should match toString() of array
        assertEquals(array.toString(), updatedB_map_key1.get("arraystring"));

	    // key1 struct should be a converted version of struct1
	    final Struct updatedB_map_key1_struct = (Struct) updatedB_map_key1.get("struct");
	    assertEquals((short) 8, updatedB_map_key1_struct.get("int8"));
	    assertEquals((long) 32, updatedB_map_key1_struct.get("int32"));
	    assertEquals(2L, updatedB_map_key1_struct.schema().field("int32").schema().defaultValue());
	    assertEquals(true, updatedB_map_key1_struct.get("float64"));
	    assertEquals(true, updatedB_map_key1_struct.schema().field("float64").schema().defaultValue());
	    assertEquals((byte) 1, updatedB_map_key1_struct.get("boolean"));
	    assertEquals(42, updatedB_map_key1_struct.get("string"));
	    assertEquals("42", updatedB_map_key1_struct.get("bigdecimal"));
	    assertNull(updatedB_map_key1_struct.get("optional"));

        // key1 structstring should match toString() of struct1
        assertEquals(struct2.toString(), updatedB_map_key1.get("structstring"));

        // And then repeat all of the above for key1, but now for key2
        final Struct updatedB_map_key2 = (Struct) updatedB_map.get("key2");
	    assertEquals(42, updatedB_map_key2.get("string"));

	    // key2 array should have 2 items converted from struct1 and struct2
        final List<Object> updatedB_map_key2_array = (List<Object>) updatedB_map_key2.get("array");
        assertEquals(2, updatedB_map_key2_array.size());

        final Struct updatedB_map_key2_array_0 = (Struct) updatedB_map_key2_array.get(0);
	    assertEquals((short) 8, updatedB_map_key2_array_0.get("int8"));
	    assertEquals((long) 32, updatedB_map_key2_array_0.get("int32"));
	    assertEquals(2L, updatedB_map_key2_array_0.schema().field("int32").schema().defaultValue());
	    assertEquals(true, updatedB_map_key2_array_0.get("float64"));
	    assertEquals(true, updatedB_map_key2_array_0.schema().field("float64").schema().defaultValue());
	    assertEquals((byte) 1, updatedB_map_key2_array_0.get("boolean"));
	    assertEquals(42, updatedB_map_key2_array_0.get("string"));
	    assertEquals("42", updatedB_map_key2_array_0.get("bigdecimal"));
	    assertNull(updatedB_map_key2_array_0.get("optional"));

        final Struct updatedB_map_key2_array_1 = (Struct) updatedB_map_key2_array.get(1);
	    assertEquals((short) 8, updatedB_map_key2_array_1.get("int8"));
	    assertEquals((long) 32, updatedB_map_key2_array_1.get("int32"));
	    assertEquals(2L, updatedB_map_key2_array_1.schema().field("int32").schema().defaultValue());
	    assertEquals(true, updatedB_map_key2_array_1.get("float64"));
	    assertEquals(true, updatedB_map_key2_array_1.schema().field("float64").schema().defaultValue());
	    assertEquals((byte) 1, updatedB_map_key2_array_1.get("boolean"));
	    assertEquals(42, updatedB_map_key2_array_1.get("string"));
	    assertEquals("42", updatedB_map_key2_array_1.get("bigdecimal"));
	    assertNull(updatedB_map_key2_array_1.get("optional"));

	    // key2 arraystring should match toString() of array
        assertEquals(array.toString(), updatedB_map_key2.get("arraystring"));

	    // key2 struct should be a converted version of struct1
	    final Struct updatedB_map_key2_struct = (Struct) updatedB_map_key2.get("struct");
	    assertEquals((short) 8, updatedB_map_key2_struct.get("int8"));
	    assertEquals((long) 32, updatedB_map_key2_struct.get("int32"));
	    assertEquals(2L, updatedB_map_key2_struct.schema().field("int32").schema().defaultValue());
	    assertEquals(true, updatedB_map_key2_struct.get("float64"));
	    assertEquals(true, updatedB_map_key2_struct.schema().field("float64").schema().defaultValue());
	    assertEquals((byte) 1, updatedB_map_key2_struct.get("boolean"));
	    assertEquals(42, updatedB_map_key2_struct.get("string"));
	    assertEquals("42", updatedB_map_key2_struct.get("bigdecimal"));
	    assertNull(updatedB_map_key2_struct.get("optional"));

        // key2 structstring should match toString() of struct1
        assertEquals(struct2.toString(), updatedB_map_key2.get("structstring"));

        // B->mapstring should match toString() of map
        assertEquals(map.toString(), updatedB.get("mapstring"));
	}

    @SuppressWarnings("unchecked")
	@Test
	public void castFieldsSchemalessRecursive() {
        Map<String, Object> config = new HashMap<>();
	    config.put(Cast.ConfigName.SPEC, "boolean:int8,int32:string,string:int32,childstring:string,parentstring:string,arraystring:string");
	    config.put(Cast.ConfigName.RECURSIVE, true);
	    xformValue.configure(config);


	    // Build and fill child
	    Map<String, Object> child = new HashMap<String, Object>();
	    child.put("boolean", true);
	    child.put("int32", 42);
	    child.put("string", "42");

	    List<Object> array = new ArrayList<Object>();
	    array.add(child);

	    // Build and fill parentA
	    Map<String, Object> parentA = new HashMap<String, Object>();
	    parentA.put("boolean", true);
	    parentA.put("int32", 42);
	    parentA.put("string", "42");
	    parentA.put("child", child);
	    parentA.put("childstring", child);
	    parentA.put("array", array);
	    parentA.put("arraystring", array);

	    // Build and fill parentB
	    Map<String, Object> parentB = new HashMap<String, Object>();
	    parentB.put("boolean", true);
	    parentB.put("int32", 42);
	    parentB.put("string", "42");
	    parentB.put("parent", parentA);
	    parentB.put("parentstring", parentA);


	    // Transform the record
	    SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
	    		null, parentB));


	    // Assert the results
        assertNull(transformed.valueSchema());

        Map<String, Object> updatedB = (Map<String, Object>) transformed.value();

        assertEquals(5, updatedB.size());
        assertEquals((byte) 1, updatedB.get("boolean"));
        assertEquals("42", updatedB.get("int32"));
        assertEquals(42, updatedB.get("string"));

        Map<String, Object> updatedA = (Map<String, Object>) updatedB.get("parent");
        assertEquals(7, updatedA.size());
        assertEquals((byte) 1, updatedA.get("boolean"));
        assertEquals("42", updatedA.get("int32"));
        assertEquals(42, updatedA.get("string"));

        Map<String, Object> updatedChild = (Map<String, Object>) updatedA.get("child");
        assertEquals(3, updatedChild.size());
        assertEquals((byte) 1, updatedChild.get("boolean"));
        assertEquals("42", updatedChild.get("int32"));
        assertEquals(42, updatedChild.get("string"));

        assertEquals(child.toString(), updatedA.get("childstring"));

        List<Object> updatedArray = (List<Object>) updatedA.get("array");
        assertEquals(1, updatedArray.size());

        Map<String, Object> updatedArray_child = (Map<String, Object>) updatedArray.get(0);
        assertEquals(3, updatedArray_child.size());
        assertEquals((byte) 1, updatedArray_child.get("boolean"));
        assertEquals("42", updatedArray_child.get("int32"));
        assertEquals(42, updatedArray_child.get("string"));

        assertEquals(array.toString(), updatedA.get("arraystring"));

        assertEquals(parentA.toString(), updatedB.get("parentstring"));
	}

}
