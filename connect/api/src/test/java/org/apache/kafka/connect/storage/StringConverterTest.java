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

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class StringConverterTest {
    private static final String TOPIC = "topic";
    private static final String SAMPLE_STRING = "a string";

    private final StringConverter converter = new StringConverter();

    @Test
    public void testStringToBytes() {
        assertArrayEquals(Utils.utf8(SAMPLE_STRING), converter.fromConnectData(TOPIC, Schema.STRING_SCHEMA, SAMPLE_STRING));
    }

    @Test
    public void testNonStringToBytes() {
        assertArrayEquals(Utils.utf8("true"), converter.fromConnectData(TOPIC, Schema.BOOLEAN_SCHEMA, true));
    }

    @Test
    public void testNullToBytes() {
        assertNull(converter.fromConnectData(TOPIC, Schema.OPTIONAL_STRING_SCHEMA, null));
    }

    @Test
    public void testToBytesIgnoresSchema() {
        assertArrayEquals(Utils.utf8("true"), converter.fromConnectData(TOPIC, null, true));
    }

    @Test
    public void testToBytesNonUtf8Encoding() {
        converter.configure(Collections.singletonMap("converter.encoding", StandardCharsets.UTF_16.name()), true);
        assertArrayEquals(SAMPLE_STRING.getBytes(StandardCharsets.UTF_16), converter.fromConnectData(TOPIC, Schema.STRING_SCHEMA, SAMPLE_STRING));
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
        assertNull(data.value());
    }

    @Test
    public void testBytesToStringNonUtf8Encoding() {
        converter.configure(Collections.singletonMap("converter.encoding", StandardCharsets.UTF_16.name()), true);
        SchemaAndValue data = converter.toConnectData(TOPIC, SAMPLE_STRING.getBytes(StandardCharsets.UTF_16));
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, data.schema());
        assertEquals(SAMPLE_STRING, data.value());
    }

    // Note: the header conversion methods delegates to the data conversion methods, which are tested above.
    // The following simply verify that the delegation works.

    @Test
    public void testStringHeaderValueToBytes() {
        assertArrayEquals(Utils.utf8(SAMPLE_STRING), converter.fromConnectHeader(TOPIC, "hdr", Schema.STRING_SCHEMA, SAMPLE_STRING));
    }

    @Test
    public void testNonStringHeaderValueToBytes() {
        assertArrayEquals(Utils.utf8("true"), converter.fromConnectHeader(TOPIC, "hdr", Schema.BOOLEAN_SCHEMA, true));
    }

    @Test
    public void testNullHeaderValueToBytes() {
        assertNull(converter.fromConnectHeader(TOPIC, "hdr", Schema.OPTIONAL_STRING_SCHEMA, null));
    }
}
