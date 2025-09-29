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
package org.apache.kafka.storage.internals.checkpoint;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.storage.internals.log.EpochEntry;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OffsetCheckpointFileWithFailureHandlerTest {

    @Test
    public void shouldPersistAndOverwriteAndReloadFile() throws IOException {

        OffsetCheckpointFile checkpoint = new OffsetCheckpointFile(TestUtils.tempFile(), null);

        //Given
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition("foo", 1), 5L);
        offsets.put(new TopicPartition("bar", 2), 10L);

        //When
        checkpoint.write(offsets);

        //Then
        assertEquals(offsets, checkpoint.read());

        //Given overwrite
        Map<TopicPartition, Long> offsets2 = new HashMap<>();
        offsets.put(new TopicPartition("foo", 2), 15L);
        offsets.put(new TopicPartition("bar", 3), 20L);

        //When
        checkpoint.write(offsets2);

        //Then
        assertEquals(offsets2, checkpoint.read());
    }

    @Test
    public void shouldHandleMultipleLines() throws IOException {

        OffsetCheckpointFile checkpoint = new OffsetCheckpointFile(TestUtils.tempFile(), null);

        //Given
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition("foo", 1), 5L);
        offsets.put(new TopicPartition("bar", 6), 10L);
        offsets.put(new TopicPartition("foo", 2), 5L);
        offsets.put(new TopicPartition("bar", 7), 10L);
        offsets.put(new TopicPartition("foo", 3), 5L);
        offsets.put(new TopicPartition("bar", 8), 10L);
        offsets.put(new TopicPartition("foo", 4), 5L);
        offsets.put(new TopicPartition("bar", 9), 10L);
        offsets.put(new TopicPartition("foo", 5), 5L);
        offsets.put(new TopicPartition("bar", 10), 10L);

        //When
        checkpoint.write(offsets);

        //Then
        assertEquals(offsets, checkpoint.read());
    }

    @Test
    public void shouldReturnEmptyMapForEmptyFile() throws IOException {

        //When
        OffsetCheckpointFile checkpoint = new OffsetCheckpointFile(TestUtils.tempFile(), null);

        //Then
        assertEquals(Collections.emptyMap(), checkpoint.read());

        //When
        checkpoint.write(Collections.emptyMap());

        //Then
        assertEquals(Collections.emptyMap(), checkpoint.read());
    }

    @Test
    public void shouldThrowIfVersionIsNotRecognised() throws IOException {
        File file = TestUtils.tempFile();
        LogDirFailureChannel logDirFailureChannel = new LogDirFailureChannel(10);
        CheckpointFileWithFailureHandler<OffsetCheckpointFile.TopicPartitionOffset> checkpointFile = new CheckpointFileWithFailureHandler<>(file, OffsetCheckpointFile.CURRENT_VERSION + 1,
                new OffsetCheckpointFile.Formatter(), logDirFailureChannel, file.getParent());
        checkpointFile.write(Collections.singletonList(new OffsetCheckpointFile.TopicPartitionOffset(new TopicPartition("foo", 5), 10L)));
        assertThrows(KafkaStorageException.class, () -> new OffsetCheckpointFile(checkpointFile.file, logDirFailureChannel).read());
    }

    @Test
    public void testLazyOffsetCheckpoint() {
        String logDir = "/tmp/kafka-logs";
        OffsetCheckpointFile mockCheckpointFile = Mockito.mock(OffsetCheckpointFile.class);

        LazyOffsetCheckpoints lazyCheckpoints = new LazyOffsetCheckpoints(Collections.singletonMap(logDir, mockCheckpointFile));
        Mockito.verify(mockCheckpointFile, Mockito.never()).read();

        TopicPartition partition0 = new TopicPartition("foo", 0);
        TopicPartition partition1 = new TopicPartition("foo", 1);
        TopicPartition partition2 = new TopicPartition("foo", 2);

        Mockito.when(mockCheckpointFile.read()).thenAnswer(invocationOnMock -> {
            Map<TopicPartition, Long> offsets = new HashMap<>();
            offsets.put(partition0, 1000L);
            offsets.put(partition1, 2000L);
            return offsets;
        });

        assertEquals(Optional.of(1000L), lazyCheckpoints.fetch(logDir, partition0));
        assertEquals(Optional.of(2000L), lazyCheckpoints.fetch(logDir, partition1));
        assertEquals(Optional.empty(), lazyCheckpoints.fetch(logDir, partition2));

        Mockito.verify(mockCheckpointFile, Mockito.times(1)).read();
    }

    @Test
    public void testLazyOffsetCheckpointFileInvalidLogDir() {
        String logDir = "/tmp/kafka-logs";
        OffsetCheckpointFile mockCheckpointFile = Mockito.mock(OffsetCheckpointFile.class);
        LazyOffsetCheckpoints lazyCheckpoints = new LazyOffsetCheckpoints(Collections.singletonMap(logDir, mockCheckpointFile));
        assertThrows(IllegalArgumentException.class, () -> lazyCheckpoints.fetch("/invalid/kafka-logs", new TopicPartition("foo", 0)));
    }

    @Test
    public void testWriteIfDirExistsShouldNotThrowWhenDirNotExists() throws IOException {
        File dir = TestUtils.tempDirectory();
        File file = dir.toPath().resolve("test-checkpoint").toFile();
        LogDirFailureChannel logDirFailureChannel = new LogDirFailureChannel(10);
        CheckpointFileWithFailureHandler<EpochEntry> checkpointFile = new CheckpointFileWithFailureHandler<>(file, 0,
                LeaderEpochCheckpointFile.FORMATTER, logDirFailureChannel, file.getParent());

        assertTrue(dir.renameTo(new File(dir.getAbsolutePath() + "-renamed")));
        checkpointFile.writeIfDirExists(Collections.singletonList(new EpochEntry(1, 42)));
    }
}
