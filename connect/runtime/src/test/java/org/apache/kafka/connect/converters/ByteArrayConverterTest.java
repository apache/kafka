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

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class ByteArrayConverterTest {
    private static final String TOPIC = "topic";
    private static final byte[] SAMPLE_BYTES = "sample string".getBytes(StandardCharsets.UTF_8);

    private final ByteArrayConverter converter = new ByteArrayConverter();

    @Before
    public void setUp() {
        converter.configure(Collections.emptyMap(), false);
    }

    @Test
    public void testFromConnect() {
        assertArrayEquals(
                SAMPLE_BYTES,
                converter.fromConnectData(TOPIC, Schema.BYTES_SCHEMA, SAMPLE_BYTES)
        );
    }

    @Test
    public void testFromConnectSchemaless() {
        assertArrayEquals(
                SAMPLE_BYTES,
                converter.fromConnectData(TOPIC, null, SAMPLE_BYTES)
        );
    }

    @Test
    public void testFromConnectBadSchema() {
        assertThrows(DataException.class,
            () -> converter.fromConnectData(TOPIC, Schema.INT32_SCHEMA, SAMPLE_BYTES));
    }

    @Test
    public void testFromConnectInvalidValue() {
        assertThrows(DataException.class,
            () -> converter.fromConnectData(TOPIC, Schema.BYTES_SCHEMA, 12));
    }

    @Test
    public void testFromConnectNull() {
        assertNull(converter.fromConnectData(TOPIC, Schema.BYTES_SCHEMA, null));
    }

    @Test
    public void testToConnect() {
        SchemaAndValue data = converter.toConnectData(TOPIC, SAMPLE_BYTES);
        assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, data.schema());
        assertArrayEquals(SAMPLE_BYTES, (byte[]) data.value());
    }

    @Test
    public void testToConnectNull() {
        SchemaAndValue data = converter.toConnectData(TOPIC, null);
        assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, data.schema());
        assertNull(data.value());
    }

    @Test
    public void testVersionRetrievedFromAppInfoParser() {
        assertEquals(AppInfoParser.getVersion(), converter.version());
    }
}
