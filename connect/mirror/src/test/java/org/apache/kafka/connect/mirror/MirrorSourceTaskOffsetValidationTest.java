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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the log-truncation ({@link DataLossException}) and topic-reset
 * ({@link TopicResetException}) detection added to {@link MirrorSourceTask}.
 */
public class MirrorSourceTaskOffsetValidationTest {

    private static final String SOURCE_CLUSTER = "primary";
    private static final TopicPartition TP0 = new TopicPartition("wal-topic", 0);
    private static final TopicPartition TP1 = new TopicPartition("wal-topic", 1);

    @SuppressWarnings("unchecked")
    private static KafkaConsumer<byte[], byte[]> mockConsumer() {
        return mock(KafkaConsumer.class);
    }

    private static MirrorSourceTask task(KafkaConsumer<byte[], byte[]> consumer, boolean offsetValidationEnabled) {
        return new MirrorSourceTask(consumer, mock(MirrorSourceLegacyMetrics.class), SOURCE_CLUSTER,
                new DefaultReplicationPolicy(), null, offsetValidationEnabled);
    }

    @Test
    public void testDataLossDetectedWhenRecordsWerePurged() {
        KafkaConsumer<byte[], byte[]> consumer = mockConsumer();
        OffsetOutOfRangeException cause = new OffsetOutOfRangeException(Map.of(TP0, 5L));
        when(consumer.poll(any())).thenThrow(cause);
        // The log now starts at offset 10, so offsets 5..9 were removed by the retention policy.
        when(consumer.beginningOffsets(anySet())).thenReturn(Map.of(TP0, 10L));

        MirrorSourceTask task = task(consumer, true);

        DataLossException e = assertThrows(DataLossException.class, task::poll);
        assertSame(cause, e.getCause(), "the original consumer exception should be preserved");
        assertTrue(e.getMessage().contains("wal-topic-0"), "message should name the topic-partition");
        assertTrue(e.getMessage().contains("offset 5"), "message should name the problematic offset");
        assertTrue(e.getMessage().contains(SOURCE_CLUSTER), "message should name the source cluster");
    }

    @Test
    public void testTopicResetDetectedWhenLogStartsAtZero() {
        KafkaConsumer<byte[], byte[]> consumer = mockConsumer();
        OffsetOutOfRangeException cause = new OffsetOutOfRangeException(Map.of(TP0, 500L));
        when(consumer.poll(any())).thenThrow(cause);
        // The log begins at 0 again: the topic was deleted and recreated.
        when(consumer.beginningOffsets(anySet())).thenReturn(Map.of(TP0, 0L));

        MirrorSourceTask task = task(consumer, true);

        TopicResetException e = assertThrows(TopicResetException.class, task::poll);
        assertSame(cause, e.getCause(), "the original consumer exception should be preserved");
        assertTrue(e.getMessage().contains("wal-topic-0"), "message should name the topic-partition");
        assertTrue(e.getMessage().contains("offset 500"), "message should name the problematic offset");
    }

    @Test
    public void testDataLossTakesPrecedenceWhenBothConditionsArePresent() {
        KafkaConsumer<byte[], byte[]> consumer = mockConsumer();
        when(consumer.poll(any())).thenThrow(new OffsetOutOfRangeException(Map.of(TP0, 5L, TP1, 500L)));
        when(consumer.beginningOffsets(anySet())).thenReturn(Map.of(TP0, 10L, TP1, 0L));

        MirrorSourceTask task = task(consumer, true);

        DataLossException e = assertThrows(DataLossException.class, task::poll);
        assertTrue(e.getMessage().contains("wal-topic-0"), "the purged partition should be reported");
    }

    @Test
    public void testUnavailableLogStartOffsetIsTreatedAsTopicReset() {
        KafkaConsumer<byte[], byte[]> consumer = mockConsumer();
        when(consumer.poll(any())).thenThrow(new OffsetOutOfRangeException(Map.of(TP0, 5L)));
        // Simulate the earliest-offset lookup failing; we must still fail the task rather than
        // fall through to the default "log a warning and carry on" behaviour.
        when(consumer.beginningOffsets(anySet())).thenThrow(new KafkaException("broker unavailable"));

        MirrorSourceTask task = task(consumer, true);

        assertThrows(TopicResetException.class, task::poll);
    }

    @Test
    public void testOffsetOutOfRangeIsNotFatalWhenValidationIsDisabled() {
        KafkaConsumer<byte[], byte[]> consumer = mockConsumer();
        when(consumer.poll(any())).thenThrow(new OffsetOutOfRangeException(Map.of(TP0, 5L)));

        MirrorSourceTask task = task(consumer, false);

        // Default MM2 behaviour: log a warning and return no records.
        assertNull(task.poll());
        verify(consumer, never()).beginningOffsets(anySet());
    }

    @Test
    public void testConsumerSeeksToBeginningForNewPartitionsWhenValidationIsEnabled() {
        KafkaConsumer<byte[], byte[]> consumer = mockConsumer();
        MirrorSourceTask task = task(consumer, true);
        task.initialize(offsetStorageContext());

        task.initializeConsumer(Set.of(TP0, TP1));

        // TP0 has a committed offset of 4, so we resume from 5.
        verify(consumer, times(1)).seek(TP0, 5L);
        // TP1 has never been replicated: auto.offset.reset=none gives the consumer no starting
        // position, so the task must seek to the beginning explicitly.
        verify(consumer, times(1)).seekToBeginning(Set.of(TP1));
    }

    @Test
    public void testConsumerDoesNotSeekToBeginningWhenValidationIsDisabled() {
        KafkaConsumer<byte[], byte[]> consumer = mockConsumer();
        MirrorSourceTask task = task(consumer, false);
        task.initialize(offsetStorageContext());

        task.initializeConsumer(Set.of(TP0, TP1));

        verify(consumer, times(1)).seek(TP0, 5L);
        // Unchanged from the default behaviour: auto.offset.reset=earliest handles new partitions.
        verify(consumer, never()).seekToBeginning(anySet());
    }

    @Test
    public void testClassificationIsIndependentOfPartitionOrdering() {
        KafkaConsumer<byte[], byte[]> consumer = mockConsumer();
        when(consumer.beginningOffsets(anySet())).thenReturn(Map.of(TP1, 0L, TP0, 0L));

        MirrorSourceTask task = task(consumer, true);
        KafkaException e = task.classifyOffsetOutOfRange(
                new OffsetOutOfRangeException(Map.of(TP1, 9L, TP0, 7L)));

        assertEquals(TopicResetException.class, e.getClass());
        assertTrue(e.getMessage().indexOf("wal-topic-0") < e.getMessage().indexOf("wal-topic-1"),
                "partitions should be reported in a deterministic order");
    }

    /**
     * A task context whose offset store has a committed offset for {@link #TP0} only.
     */
    private static SourceTaskContext offsetStorageContext() {
        SourceTaskContext context = mock(SourceTaskContext.class);
        OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(anyMap())).thenAnswer(invocation -> {
            Map<String, Object> wrappedPartition = invocation.getArgument(0);
            if (Integer.valueOf(TP0.partition()).equals(wrappedPartition.get("partition"))) {
                wrappedPartition.put("offset", 4L);
            }
            return wrappedPartition;
        });
        return context;
    }
}
