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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;

public class BooleanConverterTest {
    private static final String TOPIC = "topic";
    private static final byte[] TRUE = new byte[] {0x01};
    private static final byte[] FALSE = new byte[] {0x00};
    private final BooleanConverter converter = new BooleanConverter();

    @Before
    public void setUp() {
        converter.configure(Collections.emptyMap(), false);
    }

    @Test
    public void testFromConnect() {
        assertArrayEquals(
                TRUE,
                converter.fromConnectData(TOPIC, Schema.BOOLEAN_SCHEMA, Boolean.TRUE)
        );
        assertArrayEquals(
            FALSE,
            converter.fromConnectData(TOPIC, Schema.BOOLEAN_SCHEMA, Boolean.FALSE)
        );
    }

    @Test
    public void testFromConnectNullSchema() {
        assertArrayEquals(
            TRUE,
            converter.fromConnectData(TOPIC, null, Boolean.TRUE)
        );
        assertArrayEquals(
            FALSE,
            converter.fromConnectData(TOPIC, null, Boolean.FALSE)
        );
    }


    @Test
    public void testFromConnectWrongSchema() {
        assertThrows(DataException.class,
            () -> converter.fromConnectData(TOPIC, Schema.INT32_SCHEMA, Boolean.FALSE));
    }

    @Test
    public void testFromConnectInvalidValue() {
        assertThrows(DataException.class,
            () -> converter.fromConnectData(TOPIC, Schema.BOOLEAN_SCHEMA, "true"));
    }

    @Test
    public void testFromConnectNullValue() {
        assertNull(converter.fromConnectData(TOPIC, Schema.BOOLEAN_SCHEMA, null));
    }

    @Test
    public void testToConnect() {
        assertEquals(Schema.OPTIONAL_BOOLEAN_SCHEMA, converter.toConnectData(TOPIC, TRUE).schema());
        assertTrue((Boolean) converter.toConnectData(TOPIC, TRUE).value());

        assertEquals(Schema.OPTIONAL_BOOLEAN_SCHEMA, converter.toConnectData(TOPIC, FALSE).schema());
        assertFalse((Boolean) converter.toConnectData(TOPIC, FALSE).value());
    }

    @Test
    public void testToConnectNullValue() {
        assertEquals(Schema.OPTIONAL_BOOLEAN_SCHEMA, converter.toConnectData(TOPIC, null).schema());
        assertNull(converter.toConnectData(TOPIC, null).value());
    }

    @Test
    public void testToConnectInvalidValue() {
        byte[] invalidValue = "42".getBytes(StandardCharsets.UTF_8);
        assertThrows(DataException.class, () -> converter.toConnectData(TOPIC, invalidValue));
    }

    @Test
    public void testVersionRetrievedFromAppInfoParser() {
        assertEquals(AppInfoParser.getVersion(), converter.version());
    }
}
