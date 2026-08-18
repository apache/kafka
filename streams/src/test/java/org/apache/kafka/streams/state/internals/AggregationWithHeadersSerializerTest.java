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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.state.AggregationWithHeaders;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AggregationWithHeadersSerializerTest {

    private final Serializer<Long> longSerializer = Serdes.Long().serializer();
    private final AggregationWithHeadersSerializer<Long> serializer = new AggregationWithHeadersSerializer<>(longSerializer);

    @Test
    public void shouldSerializeNullAsNull() {
        final byte[] result = serializer.serialize("topic", null);
        assertNull(result);
    }

    @Test
    public void shouldSerializeAggregationWithEmptyHeaders() {
        final Long aggregation = 100L;
        final Headers headers = new RecordHeaders();
        final AggregationWithHeaders<Long> aggregationWithHeaders = AggregationWithHeaders.make(aggregation, headers);

        final byte[] result = serializer.serialize("topic", aggregationWithHeaders);

        assertNotNull(result);
        assertTrue(result.length > 0);
    }

    @Test
    public void shouldSerializeAggregationWithHeaders() {
        final Long aggregation = 100L;
        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        headers.add("key2", "value2".getBytes());
        final AggregationWithHeaders<Long> aggregationWithHeaders = AggregationWithHeaders.make(aggregation, headers);

        final byte[] result = serializer.serialize("topic", aggregationWithHeaders);

        assertNotNull(result);
        assertTrue(result.length > 0);
    }

    @Test
    public void shouldSerializeAndDeserializeConsistently() {
        final Long aggregation = 100L;
        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        headers.add("key2", null);
        final AggregationWithHeaders<Long> aggregationWithHeaders = AggregationWithHeaders.make(aggregation, headers);

        final byte[] serialized = serializer.serialize("topic", aggregationWithHeaders);
        final AggregationWithHeadersDeserializer<Long> deserializer = new AggregationWithHeadersDeserializer<>(Serdes.Long().deserializer());
        final AggregationWithHeaders<Long> deserialized = deserializer.deserialize("topic", serialized);

        assertNotNull(deserialized);
        assertEquals(aggregation, deserialized.aggregation());
        assertNotNull(deserialized.headers());
    }

    @Test
    public void shouldHandleNullAggregationInAggregationWithHeaders() {
        final AggregationWithHeaders<Long> aggregationWithHeaders = AggregationWithHeaders.makeAllowNullable(null, new RecordHeaders());
        final byte[] result = serializer.serialize("topic", aggregationWithHeaders);

        assertNull(result);
    }
}