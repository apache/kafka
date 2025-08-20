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

import org.apache.kafka.storage.internals.log.EpochEntry;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LeaderEpochCheckpointFileWithFailureHandlerTest {

    @Test
    public void shouldPersistAndOverwriteAndReloadFile() throws IOException {
        File file = TestUtils.tempFile("temp-checkpoint-file", String.valueOf(System.nanoTime()));

        LeaderEpochCheckpointFile checkpoint = new LeaderEpochCheckpointFile(file, new LogDirFailureChannel(1));

        //Given
        List<EpochEntry> epochs = List.of(
                new EpochEntry(0, 1L),
                new EpochEntry(1, 2L),
                new EpochEntry(2, 3L));

        //When
        checkpoint.write(epochs);

        //Then
        assertEquals(epochs, checkpoint.read());

        //Given overwrite
        List<EpochEntry> epochs2 = List.of(
                new EpochEntry(3, 4L),
                new EpochEntry(4, 5L));

        //When
        checkpoint.write(epochs2);

        //Then
        assertEquals(epochs2, checkpoint.read());
    }

    @Test
    public void shouldRetainValuesEvenIfCheckpointIsRecreated() throws IOException {
        File file = TestUtils.tempFile("temp-checkpoint-file", String.valueOf(System.nanoTime()));

        //Given a file with data in
        LeaderEpochCheckpointFile checkpoint = new LeaderEpochCheckpointFile(file, new LogDirFailureChannel(1));
        List<EpochEntry> epochs = List.of(
                new EpochEntry(0, 1L),
                new EpochEntry(1, 2L),
                new EpochEntry(2, 3L));
        checkpoint.write(epochs);

        //When we recreate
        LeaderEpochCheckpointFile checkpoint2 = new LeaderEpochCheckpointFile(file, new LogDirFailureChannel(1));

        //The data should still be there
        assertEquals(epochs, checkpoint2.read());
    }
}
