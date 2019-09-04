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

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class InsertHeaderTest {
    private InsertHeader<SourceRecord> xform = new InsertHeader<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test
    public void insertConfiguredHeaderWithStringValue() {
        final Map<String, Object> props = new HashMap<>();
        props.put("header", "dummy header");
        props.put("literal.value", "dummy value");

        xform.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test",
            0, null, null, null, null, null, null);
        final SourceRecord transformedRecord = xform.apply(record);

        Headers expected = new ConnectHeaders().addString("dummy header", "dummy value");

        assertEquals(1, transformedRecord.headers().size());
        assertEquals(expected, transformedRecord.headers());
    }

    @Test
    public void insertConfiguredHeaderWithStringValueWhenOneExists() {
        final Map<String, Object> props = new HashMap<>();
        props.put("header", "dummy header");
        props.put("literal.value", "dummy value");

        xform.configure(props);

        Headers existentHeaders = new ConnectHeaders().addString("existent key", "existent value");

        final SourceRecord record = new SourceRecord(null, null, "test",
            0, null, null, null, null, null, existentHeaders);
        final SourceRecord transformedRecord = xform.apply(record);

        Headers expected = new ConnectHeaders().addString("existent key", "existent value")
            .addString("dummy header", "dummy value");

        assertEquals(2, transformedRecord.headers().size());
        assertEquals(expected, transformedRecord.headers());
    }

    @Test
    public void insertConfiguredHeaderWithInt8Value() {
        final Map<String, Object> props = new HashMap<>();
        props.put("header", "dummy header");
        props.put("literal.value", "2");

        xform.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test",
            0, null, null, null, null, null, null);
        final SourceRecord transformedRecord = xform.apply(record);

        Headers expected = new ConnectHeaders().addByte("dummy header", (byte) 2);

        assertEquals(1, transformedRecord.headers().size());
        assertEquals(expected, transformedRecord.headers());
    }

    @Test
    public void insertConfiguredHeaderWithInt16Value() {
        final Map<String, Object> props = new HashMap<>();
        props.put("header", "dummy header");
        props.put("literal.value", "18000");

        xform.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test",
            0, null, null, null, null, null, null);
        final SourceRecord transformedRecord = xform.apply(record);

        Headers expected = new ConnectHeaders().addShort("dummy header", (short) 18000);

        assertEquals(1, transformedRecord.headers().size());
        assertEquals(expected, transformedRecord.headers());
    }

    @Test
    public void insertConfiguredHeaderWithInt32Value() {
        final Map<String, Object> props = new HashMap<>();
        props.put("header", "dummy header");
        props.put("literal.value", "50000");

        xform.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test",
            0, null, null, null, null, null, null);
        final SourceRecord transformedRecord = xform.apply(record);

        Headers expected = new ConnectHeaders().addInt("dummy header", 50000);

        assertEquals(1, transformedRecord.headers().size());
        assertEquals(expected, transformedRecord.headers());
    }

    @Test
    public void insertConfiguredHeaderWithInt64Value() {
        final Map<String, Object> props = new HashMap<>();
        props.put("header", "dummy header");
        props.put("literal.value", "87474836647");

        xform.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test",
            0, null, null, null, null, null, null);
        final SourceRecord transformedRecord = xform.apply(record);

        Headers expected = new ConnectHeaders().addLong("dummy header", 87474836647L);

        assertEquals(1, transformedRecord.headers().size());
        assertEquals(expected, transformedRecord.headers());
    }

    @Test
    public void insertConfiguredHeaderWithFloatValue() {
        final Map<String, Object> props = new HashMap<>();
        props.put("header", "dummy header");
        props.put("literal.value", "2353245343456.435435");

        xform.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test",
            0, null, null, null, null, null, null);
        final SourceRecord transformedRecord = xform.apply(record);

        Headers expected = new ConnectHeaders().addDouble("dummy header", 2353245343456.435435);

        assertEquals(1, transformedRecord.headers().size());
        assertEquals(expected, transformedRecord.headers());
    }

    @Test
    public void insertConfiguredHeaderWithDateValue() throws ParseException {
        final Map<String, Object> props = new HashMap<>();
        props.put("header", "dummy header");
        props.put("literal.value", "2019-08-23");

        xform.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test",
            0, null, null, null, null, null, null);
        final SourceRecord transformedRecord = xform.apply(record);

        SchemaAndValue schemaAndValue = new SchemaAndValue(org.apache.kafka.connect.data.Date.SCHEMA,
            new SimpleDateFormat("yyyy-MM-dd").parse("2019-08-23"));

        Headers expected = new ConnectHeaders().add("dummy header", schemaAndValue);

        assertEquals(1, transformedRecord.headers().size());
        assertEquals(expected, transformedRecord.headers());
    }

    @Test
    public void insertConfiguredHeaderWithTimeValue() throws ParseException {
        final Map<String, Object> props = new HashMap<>();
        props.put("header", "dummy header");
        props.put("literal.value", "14\\:34\\:54.346Z");
        xform.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test",
            0, null, null, null, null, null, null);
        final SourceRecord transformedRecord = xform.apply(record);

        SchemaAndValue schemaAndValue = new SchemaAndValue(Time.SCHEMA,
            new SimpleDateFormat("HH:mm:ss.SSS'Z'").parse("14:34:54.346Z"));

        Headers expected = new ConnectHeaders().add("dummy header", schemaAndValue);

        assertEquals(1, transformedRecord.headers().size());
        assertEquals(expected, transformedRecord.headers());
    }

    @Test
    public void insertConfiguredHeaderWithTimestampValue() throws ParseException {
        final Map<String, Object> props = new HashMap<>();
        props.put("header", "dummy header");
        props.put("literal.value", "2019-08-23T14\\:34\\:54.346Z");

        xform.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test",
            0, null, null, null, null, null, null);
        final SourceRecord transformedRecord = xform.apply(record);

        SchemaAndValue schemaAndValue = new SchemaAndValue(Timestamp.SCHEMA,
            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse("2019-08-23T14:34:54.346Z"));

        Headers expected = new ConnectHeaders().add("dummy header", schemaAndValue);

        assertEquals(1, transformedRecord.headers().size());
        assertEquals(expected, transformedRecord.headers());
    }
}
