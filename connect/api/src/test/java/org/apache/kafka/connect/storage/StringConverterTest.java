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
package org.apache.kafka.connect.storage;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class StringConverterTest {
    private static final String TOPIC = "topic";
    private static final String SAMPLE_STRING = "a string";

    private StringConverter converter = new StringConverter();

    @Test
    public void testStringToBytes() throws UnsupportedEncodingException {
        assertArrayEquals(SAMPLE_STRING.getBytes("UTF8"), converter.fromConnectData(TOPIC, Schema.STRING_SCHEMA, SAMPLE_STRING));
    }

    @Test
    public void testNonStringToBytes() throws UnsupportedEncodingException {
        assertArrayEquals("true".getBytes("UTF8"), converter.fromConnectData(TOPIC, Schema.BOOLEAN_SCHEMA, true));
    }

    @Test
    public void testNullToBytes() {
        assertEquals(null, converter.fromConnectData(TOPIC, Schema.OPTIONAL_STRING_SCHEMA, null));
    }

    @Test
    public void testToBytesIgnoresSchema() throws UnsupportedEncodingException {
        assertArrayEquals("true".getBytes("UTF8"), converter.fromConnectData(TOPIC, null, true));
    }

    @Test
    public void testToBytesNonUtf8Encoding() throws UnsupportedEncodingException {
        converter.configure(Collections.singletonMap("converter.encoding", "UTF-16"), true);
        assertArrayEquals(SAMPLE_STRING.getBytes("UTF-16"), converter.fromConnectData(TOPIC, Schema.STRING_SCHEMA, SAMPLE_STRING));
    }

    @Test
    public void testBytesToString() {
        SchemaAndValue data = converter.toConnectData(TOPIC, SAMPLE_STRING.getBytes());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, data.schema());
        assertEquals(SAMPLE_STRING, data.value());
    }

    @Test
    public void testBytesNullToString() {
        SchemaAndValue data = converter.toConnectData(TOPIC, null);
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, data.schema());
        assertEquals(null, data.value());
    }

    @Test
    public void testBytesToStringNonUtf8Encoding() throws UnsupportedEncodingException {
        converter.configure(Collections.singletonMap("converter.encoding", "UTF-16"), true);
        SchemaAndValue data = converter.toConnectData(TOPIC, SAMPLE_STRING.getBytes("UTF-16"));
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, data.schema());
        assertEquals(SAMPLE_STRING, data.value());
    }

    // Note: the header conversion methods delegates to the data conversion methods, which are tested above.
    // The following simply verify that the delegation works.

    @Test
    public void testStringHeaderValueToBytes() throws UnsupportedEncodingException {
        assertArrayEquals(SAMPLE_STRING.getBytes("UTF8"), converter.fromConnectHeader(TOPIC, "hdr", Schema.STRING_SCHEMA, SAMPLE_STRING));
    }

    @Test
    public void testNonStringHeaderValueToBytes() throws UnsupportedEncodingException {
        assertArrayEquals("true".getBytes("UTF8"), converter.fromConnectHeader(TOPIC, "hdr", Schema.BOOLEAN_SCHEMA, true));
    }

    @Test
    public void testNullHeaderValueToBytes() {
        assertEquals(null, converter.fromConnectHeader(TOPIC, "hdr", Schema.OPTIONAL_STRING_SCHEMA, null));
    }
}
