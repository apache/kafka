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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CastTest {
    private final Cast<SourceRecord> xformKey = new Cast.Key<>();
    private final Cast<SourceRecord> xformValue = new Cast.Value<>();
    private static final long MILLIS_PER_HOUR = TimeUnit.HOURS.toMillis(1);
    private static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1);

    @AfterEach
    public void teardown() {
        xformKey.close();
        xformValue.close();
    }

    @Test
    public void testConfigEmpty() {
        assertThrows(ConfigException.class, () -> xformKey.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "")));
    }

    @Test
    public void testConfigInvalidSchemaType() {
        assertThrows(ConfigException.class, () -> xformKey.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "foo:faketype")));
    }

    @Test
    public void testConfigInvalidTargetType() {
        assertThrows(ConfigException.class, () -> xformKey.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "foo:array")));
    }

    @Test
    public void testUnsupportedTargetType() {
        assertThrows(ConfigException.class, () -> xformKey.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "foo:bytes")));
    }

    @Test
    public void testConfigInvalidMap() {
        assertThrows(ConfigException.class, () -> xformKey.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "foo:int8:extra")));
    }

    @Test
    public void testConfigMixWholeAndFieldTransformation() {
        assertThrows(ConfigException.class, () -> xformKey.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "foo:int8,int32")));
    }

    @Test
    public void castNullValueRecordWithSchema() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "foo:int64"));
        SourceRecord original = new SourceRecord(null, null, "topic", 0,
            Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, null);
        SourceRecord transformed = xformValue.apply(original);
        assertEquals(original, transformed);
    }

    @Test
    public void castNullValueRecordSchemaless() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "foo:int64"));
        SourceRecord original = new SourceRecord(null, null, "topic", 0,
            Schema.STRING_SCHEMA, "key", null, null);
        SourceRecord transformed = xformValue.apply(original);
        assertEquals(original, transformed);
    }

    @Test
    public void castNullKeyRecordWithSchema() {
        xformKey.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "foo:int64"));
        SourceRecord original = new SourceRecord(null, null, "topic", 0,
            Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "value");
        SourceRecord transformed = xformKey.apply(original);
        assertEquals(original, transformed);
    }

    @Test
    public void castNullKeyRecordSchemaless() {
        xformKey.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "foo:int64"));
        SourceRecord original = new SourceRecord(null, null, "topic", 0,
            null, null, Schema.STRING_SCHEMA, "value");
        SourceRecord transformed = xformKey.apply(original);
        assertEquals(original, transformed);
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
    public void castWholeBigDecimalRecordValueWithSchemaString() {
        BigDecimal bigDecimal = new BigDecimal(42);
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "string"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                Decimal.schema(bigDecimal.scale()), bigDecimal));

        assertEquals(Schema.Type.STRING, transformed.valueSchema().type());
        assertEquals("42", transformed.value());
    }

    @Test
    public void castWholeDateRecordValueWithSchemaString() {
        Date timestamp = new Date(MILLIS_PER_DAY + 1); // day + 1msec to get a timestamp formatting.
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "string"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                Timestamp.SCHEMA, timestamp));

        assertEquals(Schema.Type.STRING, transformed.valueSchema().type());
        assertEquals(Values.dateFormatFor(timestamp).format(timestamp), transformed.value());
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

    @Test
    public void castWholeRecordValueSchemalessUnsupportedType() {
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int8"));
        assertThrows(DataException.class,
            () -> xformValue.apply(new SourceRecord(null, null, "topic", 0,
                    null, Collections.singletonList("foo"))));
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
        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG,
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

        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG,
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
    public void castFieldsWithSchema() {
        Date day = new Date(MILLIS_PER_DAY);
        byte[] byteArray = new byte[] {(byte) 0xFE, (byte) 0xDC, (byte) 0xBA, (byte) 0x98, 0x76, 0x54, 0x32, 0x10};
        ByteBuffer byteBuffer = ByteBuffer.wrap(Arrays.copyOf(byteArray, byteArray.length));

        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG,
                "int8:int16,int16:int32,int32:int64,int64:boolean,float32:float64,float64:boolean,boolean:int8,string:int32,bigdecimal:string,date:string,optional:int32,bytes:string,byteArray:string"));

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
        builder.field("bytes", Schema.BYTES_SCHEMA);
        builder.field("byteArray", Schema.BYTES_SCHEMA);

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
        recordValue.put("bytes", byteBuffer);
        recordValue.put("byteArray", byteArray);

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
        assertEquals("/ty6mHZUMhA=", ((Struct) transformed.value()).get("bytes"));
        assertEquals("/ty6mHZUMhA=", ((Struct) transformed.value()).get("byteArray"));

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
        assertEquals(Schema.STRING_SCHEMA.type(), transformedSchema.field("bytes").schema().type());
        assertEquals(Schema.STRING_SCHEMA.type(), transformedSchema.field("byteArray").schema().type());

        // The following fields are not changed
        assertEquals(Timestamp.SCHEMA.type(), transformedSchema.field("timestamp").schema().type());
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

    @Test
    public void testCastVersionRetrievedFromAppInfoParser() {
        assertEquals(AppInfoParser.getVersion(), xformKey.version());
        assertEquals(AppInfoParser.getVersion(), xformValue.version());

        assertEquals(xformKey.version(), xformValue.version());
    }

}
