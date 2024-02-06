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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.types.TaggedFields;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.generated.QuorumStateData;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.OptionalInt;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    @Test
    public void testCantReadVersionQuorumState() throws IOException {
        String jsonString = "{\"leaderId\":9990,\"leaderEpoch\":3012,\"votedId\":-1," +
                "\"appliedOffset\": 0,\"currentVoters\":[{\"voterId\":9990},{\"voterId\":9991},{\"voterId\":9992}]," +
                "\"data_version\":2}";
        assertCantReadQuorumStateVersion(jsonString);
    }

    @Test
    public void testSupportedVersion() {
        // If the next few checks fail, please check that they are compatible with previous releases of KRaft

        // Check that FileBasedStateStore supports the latest version
        assertEquals(FileBasedStateStore.HIGHEST_SUPPORTED_VERSION, QuorumStateData.HIGHEST_SUPPORTED_VERSION);
        // Check that the supported versions haven't changed
        assertEquals(0, QuorumStateData.HIGHEST_SUPPORTED_VERSION);
        assertEquals(0, QuorumStateData.LOWEST_SUPPORTED_VERSION);
        // For the latest version check that the number of tagged fields hasn't changed
        TaggedFields taggedFields = (TaggedFields) QuorumStateData.SCHEMA_0.get(6).def.type;
        assertEquals(0, taggedFields.numFields());
    }

    public void assertCantReadQuorumStateVersion(String jsonString) throws IOException {
        final File stateFile = TestUtils.tempFile();
        stateStore = new FileBasedStateStore(stateFile);

        // We initialized a state from the metadata log
        assertTrue(stateFile.exists());

        final int epoch = 3012;
        final int leaderId = 9990;
        final int follower1 = leaderId + 1;
        final int follower2 = follower1 + 1;
        Set<Integer> voters = Utils.mkSet(leaderId, follower1, follower2);
        writeToStateFile(stateFile, jsonString);

        assertThrows(UnsupportedVersionException.class, () -> {
            stateStore.readElectionState(); });

        stateStore.clear();
        assertFalse(stateFile.exists());
    }

    private void writeToStateFile(final File stateFile, String jsonString) {
        try (final FileOutputStream fileOutputStream = new FileOutputStream(stateFile);
             final BufferedWriter writer = new BufferedWriter(
                     new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8))) {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(jsonString);

            writer.write(node.toString());
            writer.flush();
            fileOutputStream.getFD().sync();

        } catch (IOException e) {
            throw new UncheckedIOException(
                    String.format("Error while writing to Quorum state file %s",
                            stateFile.getAbsolutePath()), e);
        }
    }


    @AfterEach
    public void cleanup() throws IOException {
        if (stateStore != null) {
            stateStore.clear();
        }
    }
}
