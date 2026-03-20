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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FetchTest {

    private static final TopicPartition TP0 = new TopicPartition("topic", 0);
    private static final TopicPartition TP1 = new TopicPartition("topic", 1);

    @Test
    void testAddToForPartitionFetch() {
        var records0 = List.of(new ConsumerRecord<>("topic", 0, 0, "key0", "value0"));
        var target = Fetch.forPartition(TP0, records0, true, new OffsetAndMetadata(1, Optional.empty(), ""));

        var records1 = List.of(new ConsumerRecord<>("topic", 1, 0, "key1", "value1"));
        var source = Fetch.forPartition(TP1, records1, true, new OffsetAndMetadata(1, Optional.empty(), ""));

        target.add(source);

        assertEquals(2, target.numRecords());
        assertTrue(target.records().containsKey(TP0));
        assertTrue(target.records().containsKey(TP1));
        assertTrue(target.nextOffsets().containsKey(TP0));
        assertTrue(target.nextOffsets().containsKey(TP1));
    }

    @Test
    void testAddForPartitionFetchToEmptyFetch() {
        Fetch<String, String> target = Fetch.empty();

        var records = List.of(new ConsumerRecord<>("topic", 0, 0, "key", "value"));
        var source = Fetch.forPartition(TP0, records, true, new OffsetAndMetadata(1, Optional.empty(), ""));

        target.add(source);

        assertEquals(1, target.numRecords());
        assertTrue(target.records().containsKey(TP0));
        assertTrue(target.nextOffsets().containsKey(TP0));
    }

    @Test
    void testForPartitionWithEmptyRecords() {
        var fetch = Fetch.forPartition(TP0, List.of(), true, new OffsetAndMetadata(1, Optional.empty(), ""));

        assertTrue(fetch.records().isEmpty());
        assertEquals(0, fetch.numRecords());
        assertTrue(fetch.positionAdvanced());
        assertFalse(fetch.isEmpty());
        assertTrue(fetch.nextOffsets().containsKey(TP0));
    }

    @Test
    void testAddWithSamePartitionMergesRecords() {
        var records0 = List.of(new ConsumerRecord<>("topic", 0, 0, "key0", "value0"));
        var records1 = List.of(new ConsumerRecord<>("topic", 0, 1, "key1", "value1"));
        var target = Fetch.forPartition(TP0, records0, true, new OffsetAndMetadata(1, Optional.empty(), ""));
        var source = Fetch.forPartition(TP0, records1, true, new OffsetAndMetadata(2, Optional.empty(), ""));

        target.add(source);

        assertEquals(2, target.numRecords());
        assertEquals(2, target.records().get(TP0).size());
    }

    @Test
    void testAddPropagatesPositionAdvanced() {
        var target = Fetch.forPartition(TP0, List.of(), false, new OffsetAndMetadata(0, Optional.empty(), ""));
        var source = Fetch.forPartition(TP1, List.of(), true, new OffsetAndMetadata(1, Optional.empty(), ""));

        assertFalse(target.positionAdvanced());

        target.add(source);

        assertTrue(target.positionAdvanced());
    }

    @Test
    void testForPartitionWithoutPositionAdvanced() {
        var records = List.of(new ConsumerRecord<>("topic", 0, 0, "key", "value"));
        var fetch = Fetch.forPartition(TP0, records, false, new OffsetAndMetadata(1, Optional.empty(), ""));

        assertFalse(fetch.positionAdvanced());
        assertFalse(fetch.isEmpty());
        assertEquals(1, fetch.numRecords());
    }

    @Test
    void testRecordsReturnsUnmodifiableMap() {
        var records = List.of(new ConsumerRecord<>("topic", 0, 0, "key", "value"));
        var fetch = Fetch.forPartition(TP0, records, true, new OffsetAndMetadata(1, Optional.empty(), ""));

        assertThrows(UnsupportedOperationException.class, () -> fetch.records().put(TP1, List.of()));
    }

    @Test
    void testNextOffsetsReturnsUnmodifiableMap() {
        var records = List.of(new ConsumerRecord<>("topic", 0, 0, "key", "value"));
        var fetch = Fetch.forPartition(TP0, records, true, new OffsetAndMetadata(1, Optional.empty(), ""));

        assertThrows(UnsupportedOperationException.class,
                () -> fetch.nextOffsets().put(TP1, new OffsetAndMetadata(99, Optional.empty(), "")));
    }

    @Test
    void testEmpty() {
        Fetch<String, String> fetch = Fetch.empty();
        assertTrue(fetch.isEmpty());
        assertEquals(0, fetch.numRecords());
        assertFalse(fetch.positionAdvanced());
        assertTrue(fetch.records().isEmpty());
        assertTrue(fetch.nextOffsets().isEmpty());
    }
}