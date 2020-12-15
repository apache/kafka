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
package org.apache.kafka.connect.converters;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public abstract class NumberConverterTest<T extends Number> {
    private static final String TOPIC = "topic";
    private static final String HEADER_NAME = "header";

    private T[] samples;
    private Schema schema;
    private NumberConverter<T> converter;
    private Serializer<T> serializer;

    protected abstract T[] samples();

    protected abstract NumberConverter<T> createConverter();

    protected abstract Serializer<T> createSerializer();

    protected abstract Schema schema();

    @Before
    public void setup() {
        converter = createConverter();
        serializer = createSerializer();
        schema = schema();
        samples = samples();
    }

    @Test
    public void testConvertingSamplesToAndFromBytes() throws UnsupportedOperationException {
        for (T sample : samples) {
            byte[] expected = serializer.serialize(TOPIC, sample);

            // Data conversion
            assertArrayEquals(expected, converter.fromConnectData(TOPIC, schema, sample));
            SchemaAndValue data = converter.toConnectData(TOPIC, expected);
            assertEquals(schema, data.schema());
            assertEquals(sample, data.value());

            // Header conversion
            assertArrayEquals(expected, converter.fromConnectHeader(TOPIC, HEADER_NAME, schema, sample));
            data = converter.toConnectHeader(TOPIC, HEADER_NAME, expected);
            assertEquals(schema, data.schema());
            assertEquals(sample, data.value());
        }
    }

    @Test(expected = DataException.class)
    public void testDeserializingDataWithTooManyBytes() {
        converter.toConnectData(TOPIC, new byte[10]);
    }

    @Test(expected = DataException.class)
    public void testDeserializingHeaderWithTooManyBytes() {
        converter.toConnectHeader(TOPIC, HEADER_NAME, new byte[10]);
    }

    @Test(expected = DataException.class)
    public void testSerializingIncorrectType() {
        converter.fromConnectData(TOPIC, schema, "not a valid number");
    }

    @Test(expected = DataException.class)
    public void testSerializingIncorrectHeader() {
        converter.fromConnectHeader(TOPIC, HEADER_NAME, schema, "not a valid number");
    }

    @Test
    public void testNullToBytes() {
        assertEquals(null, converter.fromConnectData(TOPIC, schema, null));
    }

    @Test
    public void testBytesNullToNumber() {
        SchemaAndValue data = converter.toConnectData(TOPIC, null);
        assertEquals(schema(), data.schema());
        assertNull(data.value());
    }
}
