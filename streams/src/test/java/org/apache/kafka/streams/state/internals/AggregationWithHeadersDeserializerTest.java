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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.AggregationWithHeaders;

import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AggregationWithHeadersDeserializerTest {

    private final Deserializer<Long> longDeserializer = Serdes.Long().deserializer();
    private final AggregationWithHeadersDeserializer<Long> deserializer = new AggregationWithHeadersDeserializer<>(longDeserializer);

    @Test
    public void shouldDeserializeNullAsNull() {
        final AggregationWithHeaders<Long> result = deserializer.deserialize("topic", null);
        assertNull(result);
    }

    @Test
    public void shouldDeserializeAggregationWithEmptyHeaders() {
        final Long aggregation = 100L;
        final Headers headers = new RecordHeaders();
        final AggregationWithHeaders<Long> aggregationWithHeaders = AggregationWithHeaders.make(aggregation, headers);

        final AggregationWithHeadersSerializer<Long> serializer = new AggregationWithHeadersSerializer<>(Serdes.Long().serializer());
        final byte[] serialized = serializer.serialize("topic", aggregationWithHeaders);

        final AggregationWithHeaders<Long> result = deserializer.deserialize("topic", serialized);

        assertNotNull(result);
        assertEquals(aggregation, result.aggregation());
        assertNotNull(result.headers());
    }

    @Test
    public void shouldDeserializeAggregationWithHeaders() {
        final Long aggregation = 100L;
        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        headers.add("key2", "value2".getBytes());
        final AggregationWithHeaders<Long> aggregationWithHeaders = AggregationWithHeaders.make(aggregation, headers);

        final AggregationWithHeadersSerializer<Long> serializer = new AggregationWithHeadersSerializer<>(Serdes.Long().serializer());
        final byte[] serialized = serializer.serialize("topic", aggregationWithHeaders);

        final AggregationWithHeaders<Long> result = deserializer.deserialize("topic", serialized);

        assertNotNull(result);
        assertEquals(aggregation, result.aggregation());
        assertNotNull(result.headers());

        final Iterator<Header> iterator = result.headers().iterator();
        final Header header1 = iterator.next();
        assertEquals("key1", header1.key());
        assertArrayEquals("value1".getBytes(), header1.value());

        final Header header2 = iterator.next();
        assertEquals("key2", header2.key());
        assertArrayEquals("value2".getBytes(), header2.value());
    }

    @Test
    public void shouldThrowOnInvalidFormat() {
        final byte[] invalidData = new byte[]{0x01, 0x02};
        assertThrows(SerializationException.class, () -> deserializer.deserialize("topic", invalidData));
    }

    @Test
    public void shouldExtractHeaders() {
        final Long aggregation = 100L;
        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        final AggregationWithHeaders<Long> aggregationWithHeaders = AggregationWithHeaders.make(aggregation, headers);

        final AggregationWithHeadersSerializer<Long> serializer = new AggregationWithHeadersSerializer<>(Serdes.Long().serializer());
        final byte[] serialized = serializer.serialize("topic", aggregationWithHeaders);

        final Headers extractedHeaders = AggregationWithHeadersDeserializer.headers(serialized);
        assertNotNull(extractedHeaders);

        final Header header = extractedHeaders.iterator().next();
        assertEquals("key1", header.key());
        assertArrayEquals("value1".getBytes(), header.value());
    }

    @Test
    public void shouldReturnNullForNullInput() {
        assertNull(AggregationWithHeadersDeserializer.headers(null));
    }
}