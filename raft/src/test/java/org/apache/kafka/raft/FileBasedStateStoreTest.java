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
package org.apache.kafka.raft;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;

import java.io.File;
import java.io.IOException;
import java.util.OptionalInt;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FileBasedStateStoreTest {

    private FileBasedStateStore stateStore;

    @Test
    public void testReadElectionState() throws IOException {
        final File stateFile = TestUtils.tempFile();

        stateStore = new FileBasedStateStore(stateFile);

        final int leaderId = 1;
        final int epoch = 2;
        Set<Integer> voters = Utils.mkSet(leaderId);

        stateStore.writeElectionState(ElectionState.withElectedLeader(epoch, leaderId, voters));
        assertTrue(stateFile.exists());
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), stateStore.readElectionState());

        // Start another state store and try to read from the same file.
        final FileBasedStateStore secondStateStore = new FileBasedStateStore(stateFile);
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), secondStateStore.readElectionState());
    }

    @Test
    public void testWriteElectionState() throws IOException {
        final File stateFile = TestUtils.tempFile();

        stateStore = new FileBasedStateStore(stateFile);

        // We initialized a state from the metadata log
        assertTrue(stateFile.exists());

        // The temp file should be removed
        final File createdTempFile = new File(stateFile.getAbsolutePath() + ".tmp");
        assertFalse(createdTempFile.exists());

        final int epoch = 2;
        final int leaderId = 1;
        final int votedId = 5;
        Set<Integer> voters = Utils.mkSet(leaderId, votedId);

        stateStore.writeElectionState(ElectionState.withElectedLeader(epoch, leaderId, voters));

        assertEquals(stateStore.readElectionState(), new ElectionState(epoch,
            OptionalInt.of(leaderId), OptionalInt.empty(), voters));

        stateStore.writeElectionState(ElectionState.withVotedCandidate(epoch, votedId, voters));

        assertEquals(stateStore.readElectionState(), new ElectionState(epoch,
            OptionalInt.empty(), OptionalInt.of(votedId), voters));

        final FileBasedStateStore rebootStateStore = new FileBasedStateStore(stateFile);

        assertEquals(rebootStateStore.readElectionState(), new ElectionState(epoch,
            OptionalInt.empty(), OptionalInt.of(votedId), voters));

        stateStore.clear();
        assertFalse(stateFile.exists());
    }

    @AfterEach
    public void cleanup() throws IOException {
        if (stateStore != null) {
            stateStore.clear();
        }
    }
}
