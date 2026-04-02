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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ValueWithHeadersTest {

    @Test
    public void shouldCreateValueWithHeaders() {
        final Long value = 100L;
        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());

        final ValueWithHeaders<Long> valueWithHeaders = ValueWithHeaders.make(value, headers);

        assertNotNull(valueWithHeaders);
        assertEquals(value, valueWithHeaders.value());
        assertEquals(headers, valueWithHeaders.headers());
    }

    @Test
    public void shouldReturnNullForNullValue() {
        final ValueWithHeaders<Long> valueWithHeaders = ValueWithHeaders.make(null, new RecordHeaders());
        assertNull(valueWithHeaders);
    }

    @Test
    public void shouldNotCreateWithNullHeaders() {
        final Long value = 100L;
        assertThrows(NullPointerException.class, () -> ValueWithHeaders.make(value, null));
    }

    @Test
    public void shouldAllowNullableValue() {
        final ValueWithHeaders<Long> valueWithHeaders = ValueWithHeaders.makeAllowNullable(null, new RecordHeaders());

        assertNotNull(valueWithHeaders);
        assertNull(valueWithHeaders.value());
    }

    @Test
    public void shouldGetValueOrNull() {
        final Long value = 100L;
        final ValueWithHeaders<Long> valueWithHeaders = ValueWithHeaders.make(value, new RecordHeaders());

        assertEquals(value, ValueWithHeaders.getValueOrNull(valueWithHeaders));
        assertNull(ValueWithHeaders.getValueOrNull(null));
    }

    @Test
    public void shouldImplementEquals() {
        final Long value = 100L;
        final Headers headers1 = new RecordHeaders();
        headers1.add("key1", "value1".getBytes());

        final Headers headers2 = new RecordHeaders();
        headers2.add("key1", "value1".getBytes());

        final ValueWithHeaders<Long> valueWithHeaders1 = ValueWithHeaders.make(value, headers1);
        final ValueWithHeaders<Long> valueWithHeaders2 = ValueWithHeaders.make(value, headers2);

        assertEquals(valueWithHeaders1, valueWithHeaders2);
        assertEquals(valueWithHeaders1.hashCode(), valueWithHeaders2.hashCode());
    }

    @Test
    public void shouldNotBeEqualWithDifferentValues() {
        final Headers headers = new RecordHeaders();

        final ValueWithHeaders<Long> valueWithHeaders1 = ValueWithHeaders.make(100L, headers);
        final ValueWithHeaders<Long> valueWithHeaders2 = ValueWithHeaders.make(200L, headers);

        assertNotEquals(valueWithHeaders1, valueWithHeaders2);
    }

    @Test
    public void shouldNotBeEqualWithDifferentHeaders() {
        final Long value = 100L;

        final Headers headers1 = new RecordHeaders();
        headers1.add("key1", "value1".getBytes());

        final Headers headers2 = new RecordHeaders();
        headers2.add("key2", "value2".getBytes());

        final ValueWithHeaders<Long> valueWithHeaders1 = ValueWithHeaders.make(value, headers1);
        final ValueWithHeaders<Long> valueWithHeaders2 = ValueWithHeaders.make(value, headers2);

        assertNotEquals(valueWithHeaders1, valueWithHeaders2);
    }

    @Test
    public void shouldImplementToString() {
        final Long value = 100L;
        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());

        final ValueWithHeaders<Long> valueWithHeaders = ValueWithHeaders.make(value, headers);
        final String toString = valueWithHeaders.toString();

        assertNotNull(toString);
        assertTrue(toString.contains("value=100"));
        assertTrue(toString.contains("headers="));
    }
}