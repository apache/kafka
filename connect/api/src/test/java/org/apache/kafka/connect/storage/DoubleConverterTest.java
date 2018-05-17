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

import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.Test;

import java.io.UnsupportedEncodingException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class DoubleConverterTest {
    private static final String TOPIC = "topic";
    private static final Double SAMPLE_VALUE = 1234.31d;
    private static final byte[] SAMPLE_VALUE_BYTES = new DoubleSerializer().serialize(TOPIC, SAMPLE_VALUE);
    private static final Schema SAMPLE_SCHEMA = Schema.OPTIONAL_FLOAT64_SCHEMA;
    private static final DoubleConverter CONVERTER = new DoubleConverter();

    @Test
    public void testNumberToBytes() throws UnsupportedEncodingException {
        assertArrayEquals(SAMPLE_VALUE_BYTES, CONVERTER.fromConnectData(TOPIC, SAMPLE_SCHEMA, SAMPLE_VALUE));
    }

    @Test
    public void testNullToBytes() {
        assertEquals(null, CONVERTER.fromConnectData(TOPIC, SAMPLE_SCHEMA, null));
    }

    @Test
    public void testBytesToNumber() {
        SchemaAndValue data = CONVERTER.toConnectData(TOPIC, SAMPLE_VALUE_BYTES);
        assertEquals(SAMPLE_SCHEMA, data.schema());
        assertEquals(SAMPLE_VALUE, data.value());
    }

    @Test
    public void testBytesNullToNumber() {
        SchemaAndValue data = CONVERTER.toConnectData(TOPIC, null);
        assertEquals(SAMPLE_SCHEMA, data.schema());
        assertEquals(null, data.value());
    }

    // Note: the header conversion methods delegates to the data conversion methods, which are tested above.
    // The following simply verify that the delegation works.

    @Test
    public void testHeaderValueToBytes() throws UnsupportedEncodingException {
        assertArrayEquals(SAMPLE_VALUE_BYTES, CONVERTER.fromConnectHeader(TOPIC, "hdr", SAMPLE_SCHEMA, SAMPLE_VALUE));
    }

    @Test
    public void testNullHeaderValueToBytes() {
        assertEquals(null, CONVERTER.fromConnectHeader(TOPIC, "hdr", SAMPLE_SCHEMA, null));
    }
}
