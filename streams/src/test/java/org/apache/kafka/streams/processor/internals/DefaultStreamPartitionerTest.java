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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultStreamPartitionerTest {

    private static final String TOPIC = "topic";
    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final int NUM_PARTITIONS = 5;

    @SuppressWarnings("unchecked")
    @Test
    public void shouldPropagateHeadersToSerializer() {
        final Serializer<String> keySerializer = mock(Serializer.class);
        final DefaultStreamPartitioner<String, String> defaultStreamPartitioner = new DefaultStreamPartitioner<>(keySerializer);
        final Headers headers = new RecordHeaders();
        headers.add("key", "value".getBytes());
        final byte[] expectedKeyBytes = "serializedKey".getBytes();

        when(keySerializer.serialize(TOPIC, headers, KEY)).thenReturn(expectedKeyBytes);

        final Optional<Set<Integer>> partition = defaultStreamPartitioner.partitions(TOPIC, KEY, VALUE, headers, NUM_PARTITIONS);

        verify(keySerializer).serialize(TOPIC, headers, KEY);
        assertTrue(partition.isPresent());
        assertEquals(Collections.singleton(BuiltInPartitioner.partitionForKey(expectedKeyBytes, NUM_PARTITIONS)), partition.get());
    }

    @SuppressWarnings({"unchecked", "removal"})
    @Test
    public void shouldFallbackToEmptyHeadersForDeprecatedMethod() {
        final Serializer<String> keySerializer = mock(Serializer.class);
        final DefaultStreamPartitioner<String, String> defaultStreamPartitioner = new DefaultStreamPartitioner<>(keySerializer);
        final byte[] expectedKeyBytes = "serializedKey".getBytes();
        final RecordHeaders emptyHeaders = new RecordHeaders();

        when(keySerializer.serialize(TOPIC, emptyHeaders, KEY)).thenReturn(expectedKeyBytes);

        final Optional<Set<Integer>> partition = defaultStreamPartitioner.partitions(TOPIC, KEY, VALUE, NUM_PARTITIONS);

        verify(keySerializer).serialize(TOPIC, emptyHeaders, KEY);
        assertTrue(partition.isPresent());
        assertEquals(Collections.singleton(BuiltInPartitioner.partitionForKey(expectedKeyBytes, NUM_PARTITIONS)), partition.get());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldReturnEmptyOptionalWhenSerializedKeyIsNull() {
        final Serializer<String> keySerializer = mock(Serializer.class);
        final DefaultStreamPartitioner<String, String> defaultStreamPartitioner = new DefaultStreamPartitioner<>(keySerializer);
        final Headers headers = new RecordHeaders();

        when(keySerializer.serialize(TOPIC, headers, KEY)).thenReturn(null);

        final Optional<Set<Integer>> partition = defaultStreamPartitioner.partitions(TOPIC, KEY, VALUE, headers, NUM_PARTITIONS);

        verify(keySerializer).serialize(TOPIC, headers, KEY);
        assertFalse(partition.isPresent());
    }
}
