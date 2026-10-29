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

public class AggregationWithHeadersTest {

    @Test
    public void shouldCreateAggregationWithHeaders() {
        final Long aggregation = 100L;
        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());

        final AggregationWithHeaders<Long> aggregationWithHeaders = AggregationWithHeaders.make(aggregation, headers);

        assertNotNull(aggregationWithHeaders);
        assertEquals(aggregation, aggregationWithHeaders.aggregation());
        assertEquals(headers, aggregationWithHeaders.headers());
    }

    @Test
    public void shouldReturnNullForNullAggregation() {
        final AggregationWithHeaders<Long> aggregationWithHeaders = AggregationWithHeaders.make(null, new RecordHeaders());
        assertNull(aggregationWithHeaders);
    }

    @Test
    public void shouldNotCreateWithNullHeaders() {
        final Long aggregation = 100L;
        assertThrows(NullPointerException.class, () -> AggregationWithHeaders.make(aggregation, null));
    }

    @Test
    public void shouldAllowNullableAggregation() {
        final AggregationWithHeaders<Long> aggregationWithHeaders = AggregationWithHeaders.makeAllowNullable(null, new RecordHeaders());

        assertNotNull(aggregationWithHeaders);
        assertNull(aggregationWithHeaders.aggregation());
    }

    @Test
    public void shouldGetAggregationOrNull() {
        final Long aggregation = 100L;
        final AggregationWithHeaders<Long> aggregationWithHeaders = AggregationWithHeaders.make(aggregation, new RecordHeaders());

        assertEquals(aggregation, AggregationWithHeaders.getAggregationOrNull(aggregationWithHeaders));
        assertNull(AggregationWithHeaders.getAggregationOrNull(null));
    }

    @Test
    public void shouldImplementEquals() {
        final Long aggregation = 100L;
        final Headers headers1 = new RecordHeaders();
        headers1.add("key1", "value1".getBytes());

        final Headers headers2 = new RecordHeaders();
        headers2.add("key1", "value1".getBytes());

        final AggregationWithHeaders<Long> aggregationWithHeaders1 = AggregationWithHeaders.make(aggregation, headers1);
        final AggregationWithHeaders<Long> aggregationWithHeaders2 = AggregationWithHeaders.make(aggregation, headers2);

        assertEquals(aggregationWithHeaders1, aggregationWithHeaders2);
        assertEquals(aggregationWithHeaders1.hashCode(), aggregationWithHeaders2.hashCode());
    }

    @Test
    public void shouldNotBeEqualWithDifferentAggregations() {
        final Headers headers = new RecordHeaders();

        final AggregationWithHeaders<Long> aggregationWithHeaders1 = AggregationWithHeaders.make(100L, headers);
        final AggregationWithHeaders<Long> aggregationWithHeaders2 = AggregationWithHeaders.make(200L, headers);

        assertNotEquals(aggregationWithHeaders1, aggregationWithHeaders2);
    }

    @Test
    public void shouldNotBeEqualWithDifferentHeaders() {
        final Long aggregation = 100L;

        final Headers headers1 = new RecordHeaders();
        headers1.add("key1", "value1".getBytes());

        final Headers headers2 = new RecordHeaders();
        headers2.add("key2", "value2".getBytes());

        final AggregationWithHeaders<Long> aggregationWithHeaders1 = AggregationWithHeaders.make(aggregation, headers1);
        final AggregationWithHeaders<Long> aggregationWithHeaders2 = AggregationWithHeaders.make(aggregation, headers2);

        assertNotEquals(aggregationWithHeaders1, aggregationWithHeaders2);
    }

    @Test
    public void shouldImplementToString() {
        final Long aggregation = 100L;
        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());

        final AggregationWithHeaders<Long> aggregationWithHeaders = AggregationWithHeaders.make(aggregation, headers);
        final String toString = aggregationWithHeaders.toString();

        assertNotNull(toString);
        assertTrue(toString.contains("aggregation=100"));
        assertTrue(toString.contains("headers="));
    }
}