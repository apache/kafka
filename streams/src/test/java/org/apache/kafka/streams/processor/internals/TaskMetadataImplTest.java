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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.TaskMetadata;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotEquals;


public class TaskMetadataImplTest {

    public static final TaskId TASK_ID = new TaskId(1, 2);
    public static final TopicPartition TP_0 = new TopicPartition("t", 0);
    public static final TopicPartition TP_1 = new TopicPartition("t", 1);
    public static final Set<TopicPartition> TOPIC_PARTITIONS = mkSet(TP_0, TP_1);
    public static final Map<TopicPartition, Long> COMMITTED_OFFSETS = mkMap(mkEntry(TP_1, 1L), mkEntry(TP_1, 2L));
    public static final Map<TopicPartition, Long> END_OFFSETS = mkMap(mkEntry(TP_1, 1L), mkEntry(TP_1, 3L));
    public static final Optional<Long> TIME_CURRENT_IDLING_STARTED = Optional.of(3L);

    private TaskMetadata taskMetadata;

    @Before
    public void setUp() {
        taskMetadata = new TaskMetadataImpl(
                TASK_ID,
                TOPIC_PARTITIONS,
                COMMITTED_OFFSETS,
                END_OFFSETS,
                TIME_CURRENT_IDLING_STARTED);
    }

    @Test
    public void shouldNotAllowModificationOfInternalStateViaGetters() {
        assertTrue(isUnmodifiable(taskMetadata.topicPartitions()));
        assertTrue(isUnmodifiable(taskMetadata.committedOffsets()));
        assertTrue(isUnmodifiable(taskMetadata.endOffsets()));
    }

    @Test
    public void shouldFollowEqualsAndHasCodeContract() {
        final TaskMetadataImpl same = new TaskMetadataImpl(
                TASK_ID,
                TOPIC_PARTITIONS,
                COMMITTED_OFFSETS,
                END_OFFSETS,
                TIME_CURRENT_IDLING_STARTED);
        assertEquals(taskMetadata, same);
        assertEquals(taskMetadata.hashCode(), same.hashCode());

        final TaskMetadataImpl stillSameDifferCommittedOffsets = new TaskMetadataImpl(
                TASK_ID,
                TOPIC_PARTITIONS,
                mkMap(mkEntry(TP_1, 1000000L), mkEntry(TP_1, 2L)),
                END_OFFSETS,
                TIME_CURRENT_IDLING_STARTED);
        assertEquals(taskMetadata, stillSameDifferCommittedOffsets);
        assertEquals(taskMetadata.hashCode(), stillSameDifferCommittedOffsets.hashCode());

        final TaskMetadataImpl stillSameDifferEndOffsets = new TaskMetadataImpl(
                TASK_ID,
                TOPIC_PARTITIONS,
                COMMITTED_OFFSETS,
                mkMap(mkEntry(TP_1, 1000000L), mkEntry(TP_1, 2L)),
                TIME_CURRENT_IDLING_STARTED);
        assertEquals(taskMetadata, stillSameDifferEndOffsets);
        assertEquals(taskMetadata.hashCode(), stillSameDifferEndOffsets.hashCode());

        final TaskMetadataImpl stillSameDifferIdlingTime = new TaskMetadataImpl(
                TASK_ID,
                TOPIC_PARTITIONS,
                COMMITTED_OFFSETS,
                END_OFFSETS,
                Optional.empty());
        assertEquals(taskMetadata, stillSameDifferIdlingTime);
        assertEquals(taskMetadata.hashCode(), stillSameDifferIdlingTime.hashCode());

        final TaskMetadataImpl differTaskId = new TaskMetadataImpl(
                new TaskId(1, 10000),
                TOPIC_PARTITIONS,
                COMMITTED_OFFSETS,
                END_OFFSETS,
                TIME_CURRENT_IDLING_STARTED);
        assertNotEquals(taskMetadata, differTaskId);
        assertNotEquals(taskMetadata.hashCode(), differTaskId.hashCode());

        final TaskMetadataImpl differTopicPartitions = new TaskMetadataImpl(
                TASK_ID,
                mkSet(TP_0),
                COMMITTED_OFFSETS,
                END_OFFSETS,
                TIME_CURRENT_IDLING_STARTED);
        assertNotEquals(taskMetadata, differTopicPartitions);
        assertNotEquals(taskMetadata.hashCode(), differTopicPartitions.hashCode());
    }

    private static boolean isUnmodifiable(final Collection<?> collection) {
        try {
            collection.clear();
            return false;
        } catch (final UnsupportedOperationException e) {
            return true;
        }
    }

    private static boolean isUnmodifiable(final Map<?, ?> collection) {
        try {
            collection.clear();
            return false;
        } catch (final UnsupportedOperationException e) {
            return true;
        }
    }
}
