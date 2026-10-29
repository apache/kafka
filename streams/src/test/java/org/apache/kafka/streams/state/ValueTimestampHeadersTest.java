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
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ValueTimestampHeadersTest {

    private static final String VALUE = "test-value";
    private static final long TIMESTAMP = 123456789L;

    @Test
    public void shouldCreateInstanceWithMake() {
        final Headers headers = new RecordHeaders().add("key1", "value1".getBytes());
        final ValueTimestampHeaders<String> valueTimestampHeaders = ValueTimestampHeaders.make(VALUE, TIMESTAMP, headers);

        assertNotNull(valueTimestampHeaders);
        assertEquals(VALUE, valueTimestampHeaders.value());
        assertEquals(TIMESTAMP, valueTimestampHeaders.timestamp());
        assertEquals(headers, valueTimestampHeaders.headers());
    }

    @Test
    public void shouldReturnNullWhenValueIsNullWithMake() {
        final Headers headers = new RecordHeaders().add("key1", "value1".getBytes());
        final ValueTimestampHeaders<String> valueTimestampHeaders = ValueTimestampHeaders.make(null, TIMESTAMP, headers);

        assertNull(valueTimestampHeaders);
    }

    @Test
    public void shouldCreateInstanceWithMakeAllowNullable() {
        final Headers headers = new RecordHeaders().add("key1", "value1".getBytes());
        final ValueTimestampHeaders<String> valueTimestampHeaders = ValueTimestampHeaders.makeAllowNullable(VALUE, TIMESTAMP, headers);

        assertNotNull(valueTimestampHeaders);
        assertEquals(VALUE, valueTimestampHeaders.value());
        assertEquals(TIMESTAMP, valueTimestampHeaders.timestamp());
        assertEquals(headers, valueTimestampHeaders.headers());
    }

    @Test
    public void shouldCreateInstanceWithNullValueUsingMakeAllowNullable() {
        final Headers headers = new RecordHeaders().add("key1", "value1".getBytes());
        final ValueTimestampHeaders<String> valueTimestampHeaders = ValueTimestampHeaders.makeAllowNullable(null, TIMESTAMP, headers);

        assertNotNull(valueTimestampHeaders);
        assertNull(valueTimestampHeaders.value());
        assertEquals(TIMESTAMP, valueTimestampHeaders.timestamp());
        assertEquals(headers, valueTimestampHeaders.headers());
    }

    @Test
    public void shouldCreateEmptyHeadersWhenHeadersAreNull() {
        final ValueTimestampHeaders<String> valueTimestampHeaders = ValueTimestampHeaders.make(VALUE, TIMESTAMP, null);

        assertNotNull(valueTimestampHeaders);
        assertNotNull(valueTimestampHeaders.headers());
        assertEquals(0, valueTimestampHeaders.headers().toArray().length);
    }

    @Test
    public void shouldGetValueOrNull() {
        final Headers headers = new RecordHeaders();
        ValueTimestampHeaders<String> valueTimestampHeaders = ValueTimestampHeaders.make(VALUE, TIMESTAMP, headers);
        assertEquals(VALUE, ValueTimestampHeaders.getValueOrNull(valueTimestampHeaders));

        valueTimestampHeaders = ValueTimestampHeaders.makeAllowNullable(null, TIMESTAMP, headers);
        assertNull(ValueTimestampHeaders.getValueOrNull(valueTimestampHeaders));

        assertNull(ValueTimestampHeaders.getValueOrNull(null));
    }

    @Test
    public void shouldBeEqualWhenSameValues() {
        final Headers headers1 = new RecordHeaders().add("key1", "value1".getBytes());
        final Headers headers2 = new RecordHeaders().add("key1", "value1".getBytes());

        final ValueTimestampHeaders<String> valueTimestampHeaders1 = ValueTimestampHeaders.make(VALUE, TIMESTAMP, headers1);
        final ValueTimestampHeaders<String> valueTimestampHeaders2 = ValueTimestampHeaders.make(VALUE, TIMESTAMP, headers2);

        assertEquals(valueTimestampHeaders1, valueTimestampHeaders2);
        assertEquals(valueTimestampHeaders1.hashCode(), valueTimestampHeaders2.hashCode());
    }

    @Test
    public void shouldNotBeEqualWhenDifferentValues() {
        final Headers headers = new RecordHeaders().add("key1", "value1".getBytes());

        final ValueTimestampHeaders<String> valueTimestampHeaders1 = ValueTimestampHeaders.make(VALUE, TIMESTAMP, headers);
        final ValueTimestampHeaders<String> valueTimestampHeaders2 = ValueTimestampHeaders.make("different", TIMESTAMP, headers);

        assertNotEquals(valueTimestampHeaders1, valueTimestampHeaders2);
    }

    @Test
    public void shouldNotBeEqualWhenDifferentTimestamps() {
        final Headers headers = new RecordHeaders().add("key1", "value1".getBytes());

        final ValueTimestampHeaders<String> valueTimestampHeaders1 = ValueTimestampHeaders.make(VALUE, TIMESTAMP, headers);
        final ValueTimestampHeaders<String> valueTimestampHeaders2 = ValueTimestampHeaders.make(VALUE, TIMESTAMP + 1, headers);

        assertNotEquals(valueTimestampHeaders1, valueTimestampHeaders2);
    }

    @Test
    public void shouldNotBeEqualWhenDifferentHeaders() {
        final Headers headers1 = new RecordHeaders().add("key1", "value1".getBytes());
        final Headers headers2 = new RecordHeaders().add("key2", "value2".getBytes());

        final ValueTimestampHeaders<String> valueTimestampHeaders1 = ValueTimestampHeaders.make(VALUE, TIMESTAMP, headers1);
        final ValueTimestampHeaders<String> valueTimestampHeaders2 = ValueTimestampHeaders.make(VALUE, TIMESTAMP, headers2);

        assertNotEquals(valueTimestampHeaders1, valueTimestampHeaders2);
    }

    @Test
    public void shouldBeEqualToItself() {
        final Headers headers = new RecordHeaders().add("key1", "value1".getBytes());
        final ValueTimestampHeaders<String> valueTimestampHeaders = ValueTimestampHeaders.make(VALUE, TIMESTAMP, headers);

        assertEquals(valueTimestampHeaders, valueTimestampHeaders);
    }

    @Test
    public void shouldHaveCorrectToString() {
        final Headers headers = new RecordHeaders().add("key1", "value1".getBytes());
        final ValueTimestampHeaders<String> valueTimestampHeaders = ValueTimestampHeaders.make(VALUE, TIMESTAMP, headers);

        final String toString = valueTimestampHeaders.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("value=" + VALUE));
        assertTrue(toString.contains("timestamp=" + TIMESTAMP));
    }
}
