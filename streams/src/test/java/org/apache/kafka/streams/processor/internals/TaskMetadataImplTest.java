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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class TaskMetadataImplTest {

    public static final TaskId TASK_ID = new TaskId(1, 2);
    public static final TopicPartition TP_0 = new TopicPartition("t", 0);
    public static final TopicPartition TP_1 = new TopicPartition("t", 1);
    public static final Set<TopicPartition> TOPIC_PARTITIONS = Set.of(TP_0, TP_1);
    public static final Map<TopicPartition, Long> COMMITTED_OFFSETS = mkMap(mkEntry(TP_1, 1L), mkEntry(TP_1, 2L));
    public static final Map<TopicPartition, Long> END_OFFSETS = mkMap(mkEntry(TP_1, 1L), mkEntry(TP_1, 3L));
    public static final Optional<Long> TIME_CURRENT_IDLING_STARTED = Optional.of(3L);

    private TaskMetadata taskMetadata;

    @BeforeEach
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
        assertThat(isUnmodifiable(taskMetadata.topicPartitions()), is(true));
        assertThat(isUnmodifiable(taskMetadata.committedOffsets()), is(true));
        assertThat(isUnmodifiable(taskMetadata.endOffsets()), is(true));
    }

    @Test
    public void shouldBeEqualsIfSameObject() {
        final TaskMetadataImpl same = new TaskMetadataImpl(
            TASK_ID,
            TOPIC_PARTITIONS,
            COMMITTED_OFFSETS,
            END_OFFSETS,
            TIME_CURRENT_IDLING_STARTED);
        assertThat(taskMetadata, equalTo(same));
        assertThat(taskMetadata.hashCode(), equalTo(same.hashCode()));
    }

    @Test
    public void shouldBeEqualsIfOnlyDifferInCommittedOffsets() {
        final TaskMetadataImpl stillSameDifferCommittedOffsets = new TaskMetadataImpl(
            TASK_ID,
            TOPIC_PARTITIONS,
            mkMap(mkEntry(TP_1, 1000000L), mkEntry(TP_1, 2L)),
            END_OFFSETS,
            TIME_CURRENT_IDLING_STARTED);
        assertThat(taskMetadata, equalTo(stillSameDifferCommittedOffsets));
        assertThat(taskMetadata.hashCode(), equalTo(stillSameDifferCommittedOffsets.hashCode()));
    }

    @Test
    public void shouldBeEqualsIfOnlyDifferInEndOffsets() {
        final TaskMetadataImpl stillSameDifferEndOffsets = new TaskMetadataImpl(
            TASK_ID,
            TOPIC_PARTITIONS,
            COMMITTED_OFFSETS,
            mkMap(mkEntry(TP_1, 1000000L), mkEntry(TP_1, 2L)),
            TIME_CURRENT_IDLING_STARTED);
        assertThat(taskMetadata, equalTo(stillSameDifferEndOffsets));
        assertThat(taskMetadata.hashCode(), equalTo(stillSameDifferEndOffsets.hashCode()));
    }

    @Test
    public void shouldBeEqualsIfOnlyDifferInIdlingTime() {
        final TaskMetadataImpl stillSameDifferIdlingTime = new TaskMetadataImpl(
            TASK_ID,
            TOPIC_PARTITIONS,
            COMMITTED_OFFSETS,
            END_OFFSETS,
            Optional.empty());
        assertThat(taskMetadata, equalTo(stillSameDifferIdlingTime));
        assertThat(taskMetadata.hashCode(), equalTo(stillSameDifferIdlingTime.hashCode()));
    }

    @Test
    public void shouldNotBeEqualsIfDifferInTaskID() {
        final TaskMetadataImpl differTaskId = new TaskMetadataImpl(
            new TaskId(1, 10000),
            TOPIC_PARTITIONS,
            COMMITTED_OFFSETS,
            END_OFFSETS,
            TIME_CURRENT_IDLING_STARTED);
        assertThat(taskMetadata, not(equalTo(differTaskId)));
        assertThat(taskMetadata.hashCode(), not(equalTo(differTaskId.hashCode())));
    }

    @Test
    public void shouldNotBeEqualsIfDifferInTopicPartitions() {
        final TaskMetadataImpl differTopicPartitions = new TaskMetadataImpl(
            TASK_ID,
            Set.of(TP_0),
            COMMITTED_OFFSETS,
            END_OFFSETS,
            TIME_CURRENT_IDLING_STARTED);
        assertThat(taskMetadata, not(equalTo(differTopicPartitions)));
        assertThat(taskMetadata.hashCode(), not(equalTo(differTopicPartitions.hashCode())));
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
