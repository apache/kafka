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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.types.TaggedFields;
import org.apache.kafka.raft.generated.QuorumStateData;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.test.TestUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.server.common.KRaftVersion.KRAFT_VERSION_1;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FileQuorumStateStoreTest {
    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    void testWriteReadElectedLeader(KRaftVersion kraftVersion) throws IOException {
        FileQuorumStateStore stateStore = new FileQuorumStateStore(TestUtils.tempFile());

        final int epoch = 2;
        final int voter1 = 1;
        final int voter2 = 2;
        final int voter3 = 3;
        Set<Integer> voters = Set.of(voter1, voter2, voter3);

        stateStore.writeElectionState(
            ElectionState.withElectedLeader(epoch, voter1, voters),
            kraftVersion
        );

        final Optional<ElectionState> expected;
        if (kraftVersion.isReconfigSupported()) {
            expected = Optional.of(
                ElectionState.withElectedLeader(epoch, voter1, Collections.emptySet())
            );
        } else {
            expected = Optional.of(ElectionState.withElectedLeader(epoch, voter1, voters));
        }

        assertEquals(expected, stateStore.readElectionState());

        stateStore.clear();
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    void testWriteReadVotedCandidate(KRaftVersion kraftVersion) throws IOException {
        FileQuorumStateStore stateStore = new FileQuorumStateStore(TestUtils.tempFile());

        final int epoch = 2;
        final int voter1 = 1;
        final ReplicaKey voter1Key = ReplicaKey.of(voter1, Uuid.randomUuid());
        final int voter2 = 2;
        final int voter3 = 3;
        Set<Integer> voters = Set.of(voter1, voter2, voter3);

        stateStore.writeElectionState(
            ElectionState.withVotedCandidate(epoch, voter1Key, voters),
            kraftVersion
        );

        final Optional<ElectionState> expected;
        if (kraftVersion.isReconfigSupported()) {
            expected = Optional.of(
                ElectionState.withVotedCandidate(
                    epoch,
                    voter1Key,
                    Collections.emptySet()
                )
            );
        } else {
            expected = Optional.of(
                ElectionState.withVotedCandidate(
                    epoch,
                    ReplicaKey.of(voter1, ReplicaKey.NO_DIRECTORY_ID),
                    voters
                )
            );
        }

        assertEquals(expected, stateStore.readElectionState());
        stateStore.clear();
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    void testWriteReadUnknownLeader(KRaftVersion kraftVersion) throws IOException {
        FileQuorumStateStore stateStore = new FileQuorumStateStore(TestUtils.tempFile());

        final int epoch = 2;
        Set<Integer> voters = Set.of(1, 2, 3);

        stateStore.writeElectionState(
            ElectionState.withUnknownLeader(epoch, voters),
            kraftVersion
        );

        final Optional<ElectionState> expected;
        if (kraftVersion.isReconfigSupported()) {
            expected = Optional.of(ElectionState.withUnknownLeader(epoch, Collections.emptySet()));
        } else {
            expected = Optional.of(ElectionState.withUnknownLeader(epoch, voters));
        }

        assertEquals(expected, stateStore.readElectionState());
        stateStore.clear();
    }

    @Test
    void testReload()  throws IOException {
        final File stateFile = TestUtils.tempFile();
        FileQuorumStateStore stateStore = new FileQuorumStateStore(stateFile);

        final int epoch = 2;
        Set<Integer> voters = Set.of(1, 2, 3);

        stateStore.writeElectionState(ElectionState.withUnknownLeader(epoch, voters), KRAFT_VERSION_1);

        // Check that state is persisted
        FileQuorumStateStore reloadedStore = new FileQuorumStateStore(stateFile);
        assertEquals(
            Optional.of(ElectionState.withUnknownLeader(epoch, Collections.emptySet())),
            reloadedStore.readElectionState()
        );
    }

    @Test
    void testCreateAndClear() throws IOException {
        final File stateFile = TestUtils.tempFile();
        FileQuorumStateStore stateStore = new FileQuorumStateStore(stateFile);

        // We initialized a state from the metadata log
        assertTrue(stateFile.exists());

        // The temp file should be removed
        final File createdTempFile = new File(stateFile.getAbsolutePath() + ".tmp");
        assertFalse(createdTempFile.exists());

        // Clear delete the state file
        stateStore.clear();
        assertFalse(stateFile.exists());
    }

    @Test
    public void testCantReadVersionQuorumState() throws IOException {
        String jsonString = "{\"leaderId\":9990,\"leaderEpoch\":3012,\"votedId\":-1," +
                "\"appliedOffset\": 0,\"currentVoters\":[{\"voterId\":9990},{\"voterId\":9991},{\"voterId\":9992}]," +
                "\"data_version\":2}";
        final File stateFile = TestUtils.tempFile();
        writeToStateFile(stateFile, jsonString);

        FileQuorumStateStore stateStore = new FileQuorumStateStore(stateFile);
        assertThrows(IllegalStateException.class, stateStore::readElectionState);

        stateStore.clear();
    }

    @Test
    public void testSupportedVersion() {
        // If the next few checks fail, please check that they are compatible with previous releases of KRaft

        // Check that FileQuorumStateStore supports the latest version
        assertEquals(FileQuorumStateStore.HIGHEST_SUPPORTED_VERSION, QuorumStateData.HIGHEST_SUPPORTED_VERSION);
        // Check that the supported versions haven't changed
        assertEquals(1, QuorumStateData.HIGHEST_SUPPORTED_VERSION);
        assertEquals(0, QuorumStateData.LOWEST_SUPPORTED_VERSION);
        // For the latest version check that the number of tagged fields hasn't changed
        TaggedFields taggedFields = (TaggedFields) QuorumStateData.SCHEMA_1.get(4).def.type;
        assertEquals(0, taggedFields.numFields());
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
}
