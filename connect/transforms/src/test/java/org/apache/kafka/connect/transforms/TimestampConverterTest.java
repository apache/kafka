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
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TimestampConverterTest {
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    private static final Calendar EPOCH;
    private static final Calendar TIME;
    private static final Calendar DATE;
    private static final Calendar DATE_PLUS_TIME;
    private static final long DATE_PLUS_TIME_UNIX;
    private static final String STRING_DATE_FMT = "yyyy MM dd HH mm ss SSS z";
    private static final String DATE_PLUS_TIME_STRING;

    private final TimestampConverter<SourceRecord> xformKey = new TimestampConverter.Key<>();
    private final TimestampConverter<SourceRecord> xformValue = new TimestampConverter.Value<>();

    static {
        EPOCH = GregorianCalendar.getInstance(UTC);
        EPOCH.setTimeInMillis(0L);

        TIME = GregorianCalendar.getInstance(UTC);
        TIME.setTimeInMillis(0L);
        TIME.add(Calendar.MILLISECOND, 1234);

        DATE = GregorianCalendar.getInstance(UTC);
        DATE.setTimeInMillis(0L);
        DATE.set(1970, Calendar.JANUARY, 1, 0, 0, 0);
        DATE.add(Calendar.DATE, 1);

        DATE_PLUS_TIME = GregorianCalendar.getInstance(UTC);
        DATE_PLUS_TIME.setTimeInMillis(0L);
        DATE_PLUS_TIME.add(Calendar.DATE, 1);
        DATE_PLUS_TIME.add(Calendar.MILLISECOND, 1234);

        DATE_PLUS_TIME_UNIX = DATE_PLUS_TIME.getTime().getTime();
        DATE_PLUS_TIME_STRING = "1970 01 02 00 00 01 234 UTC";
    }


    // Configuration

    @After
    public void teardown() {
        xformKey.close();
        xformValue.close();
    }

    @Test(expected = ConfigException.class)
    public void testConfigNoTargetType() {
        xformValue.configure(Collections.<String, String>emptyMap());
    }

    @Test(expected = ConfigException.class)
    public void testConfigInvalidTargetType() {
        xformValue.configure(Collections.singletonMap(TimestampConverter.TARGET_TYPE_CONFIG, "invalid"));
    }

    @Test(expected = ConfigException.class)
    public void testConfigMissingFormat() {
        xformValue.configure(Collections.singletonMap(TimestampConverter.TARGET_TYPE_CONFIG, "string"));
    }

    @Test(expected = ConfigException.class)
    public void testConfigInvalidFormat() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "string");
        config.put(TimestampConverter.FORMAT_CONFIG, "bad-format");
        xformValue.configure(config);
    }


    // Conversions without schemas (most flexible Timestamp -> other types)

    @Test
    public void testSchemalessIdentity() {
        xformValue.configure(Collections.singletonMap(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, null, DATE_PLUS_TIME.getTime()));

        assertNull(transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), transformed.value());
    }

    @Test
    public void testSchemalessTimestampToDate() {
        xformValue.configure(Collections.singletonMap(TimestampConverter.TARGET_TYPE_CONFIG, "Date"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, null, DATE_PLUS_TIME.getTime()));

        assertNull(transformed.valueSchema());
        assertEquals(DATE.getTime(), transformed.value());
    }

    @Test
    public void testSchemalessTimestampToTime() {
        xformValue.configure(Collections.singletonMap(TimestampConverter.TARGET_TYPE_CONFIG, "Time"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, null, DATE_PLUS_TIME.getTime()));

        assertNull(transformed.valueSchema());
        assertEquals(TIME.getTime(), transformed.value());
    }

    @Test
    public void testSchemalessTimestampToUnix() {
        xformValue.configure(Collections.singletonMap(TimestampConverter.TARGET_TYPE_CONFIG, "unix"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, null, DATE_PLUS_TIME.getTime()));

        assertNull(transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME_UNIX, transformed.value());
    }

    @Test
    public void testSchemalessTimestampToString() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "string");
        config.put(TimestampConverter.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, null, DATE_PLUS_TIME.getTime()));

        assertNull(transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME_STRING, transformed.value());
    }


    // Conversions without schemas (core types -> most flexible Timestamp format)

    @Test
    public void testSchemalessDateToTimestamp() {
        xformValue.configure(Collections.singletonMap(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, null, DATE.getTime()));

        assertNull(transformed.valueSchema());
        // No change expected since the source type is coarser-grained
        assertEquals(DATE.getTime(), transformed.value());
    }

    @Test
    public void testSchemalessTimeToTimestamp() {
        xformValue.configure(Collections.singletonMap(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, null, TIME.getTime()));

        assertNull(transformed.valueSchema());
        // No change expected since the source type is coarser-grained
        assertEquals(TIME.getTime(), transformed.value());
    }

    @Test
    public void testSchemalessUnixToTimestamp() {
        xformValue.configure(Collections.singletonMap(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, null, DATE_PLUS_TIME_UNIX));

        assertNull(transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), transformed.value());
    }

    @Test
    public void testSchemalessStringToTimestamp() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(TimestampConverter.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, null, DATE_PLUS_TIME_STRING));

        assertNull(transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), transformed.value());
    }


    // Conversions with schemas (most flexible Timestamp -> other types)

    @Test
    public void testWithSchemaIdentity() {
        xformValue.configure(Collections.singletonMap(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, Timestamp.SCHEMA, DATE_PLUS_TIME.getTime()));

        assertEquals(Timestamp.SCHEMA, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), transformed.value());
    }

    @Test
    public void testWithSchemaTimestampToDate() {
        xformValue.configure(Collections.singletonMap(TimestampConverter.TARGET_TYPE_CONFIG, "Date"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, Timestamp.SCHEMA, DATE_PLUS_TIME.getTime()));

        assertEquals(Date.SCHEMA, transformed.valueSchema());
        assertEquals(DATE.getTime(), transformed.value());
    }

    @Test
    public void testWithSchemaTimestampToTime() {
        xformValue.configure(Collections.singletonMap(TimestampConverter.TARGET_TYPE_CONFIG, "Time"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, Timestamp.SCHEMA, DATE_PLUS_TIME.getTime()));

        assertEquals(Time.SCHEMA, transformed.valueSchema());
        assertEquals(TIME.getTime(), transformed.value());
    }

    @Test
    public void testWithSchemaTimestampToUnix() {
        xformValue.configure(Collections.singletonMap(TimestampConverter.TARGET_TYPE_CONFIG, "unix"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, Timestamp.SCHEMA, DATE_PLUS_TIME.getTime()));

        assertEquals(Schema.INT64_SCHEMA, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME_UNIX, transformed.value());
    }

    @Test
    public void testWithSchemaTimestampToString() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "string");
        config.put(TimestampConverter.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, Timestamp.SCHEMA, DATE_PLUS_TIME.getTime()));

        assertEquals(Schema.STRING_SCHEMA, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME_STRING, transformed.value());
    }


    // Conversions with schemas (core types -> most flexible Timestamp format)

    @Test
    public void testWithSchemaDateToTimestamp() {
        xformValue.configure(Collections.singletonMap(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, Date.SCHEMA, DATE.getTime()));

        assertEquals(Timestamp.SCHEMA, transformed.valueSchema());
        // No change expected since the source type is coarser-grained
        assertEquals(DATE.getTime(), transformed.value());
    }

    @Test
    public void testWithSchemaTimeToTimestamp() {
        xformValue.configure(Collections.singletonMap(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, Time.SCHEMA, TIME.getTime()));

        assertEquals(Timestamp.SCHEMA, transformed.valueSchema());
        // No change expected since the source type is coarser-grained
        assertEquals(TIME.getTime(), transformed.value());
    }

    @Test
    public void testWithSchemaUnixToTimestamp() {
        xformValue.configure(Collections.singletonMap(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, Schema.INT64_SCHEMA, DATE_PLUS_TIME_UNIX));

        assertEquals(Timestamp.SCHEMA, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), transformed.value());
    }

    @Test
    public void testWithSchemaStringToTimestamp() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(TimestampConverter.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, Schema.STRING_SCHEMA, DATE_PLUS_TIME_STRING));

        assertEquals(Timestamp.SCHEMA, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), transformed.value());
    }


    // Convert field instead of entire key/value

    @Test
    public void testSchemalessFieldConversion() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Date");
        config.put(TimestampConverter.FIELD_CONFIG, "ts");
        xformValue.configure(config);

        Object value = Collections.singletonMap("ts", DATE_PLUS_TIME.getTime());
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, null, value));

        assertNull(transformed.valueSchema());
        assertEquals(Collections.singletonMap("ts", DATE.getTime()), transformed.value());
    }

    @Test
    public void testWithSchemaFieldConversion() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(TimestampConverter.FIELD_CONFIG, "ts");
        xformValue.configure(config);

        // ts field is a unix timestamp
        Schema structWithTimestampFieldSchema = SchemaBuilder.struct()
                .field("ts", Schema.INT64_SCHEMA)
                .field("other", Schema.STRING_SCHEMA)
                .build();
        Struct original = new Struct(structWithTimestampFieldSchema);
        original.put("ts", DATE_PLUS_TIME_UNIX);
        original.put("other", "test");

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, structWithTimestampFieldSchema, original));

        Schema expectedSchema = SchemaBuilder.struct()
                .field("ts", Timestamp.SCHEMA)
                .field("other", Schema.STRING_SCHEMA)
                .build();
        assertEquals(expectedSchema, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), ((Struct) transformed.value()).get("ts"));
        assertEquals("test", ((Struct) transformed.value()).get("other"));
    }


    // Validate Key implementation in addition to Value

    @Test
    public void testKey() {
        xformKey.configure(Collections.singletonMap(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformKey.apply(new SourceRecord(null, null, "topic", 0, null, DATE_PLUS_TIME.getTime(), null, null));

        assertNull(transformed.keySchema());
        assertEquals(DATE_PLUS_TIME.getTime(), transformed.key());
    }

}
