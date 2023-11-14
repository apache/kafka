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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.generated.QuorumStateData;
import org.apache.kafka.raft.generated.QuorumStateData.Voter;
import org.apache.kafka.raft.generated.QuorumStateDataJsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.UncheckedIOException;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Local file based quorum state store. It takes the JSON format of {@link QuorumStateData}
 * with an extra data version number as part of the data for easy deserialization.
 *
 * Example format:
 * <pre>
 * {"clusterId":"",
 *   "leaderId":1,
 *   "leaderEpoch":2,
 *   "votedId":-1,
 *   "appliedOffset":0,
 *   "currentVoters":[],
 *   "data_version":0}
 * </pre>
 * */
public class FileBasedStateStore implements QuorumStateStore {
    private static final Logger log = LoggerFactory.getLogger(FileBasedStateStore.class);

    private final File stateFile;

    static final String DATA_VERSION = "data_version";
    static final short HIGHEST_SUPPORTED_VERSION = 0;

    public FileBasedStateStore(final File stateFile) {
        this.stateFile = stateFile;
    }

    private QuorumStateData readStateFromFile(File file) {
        try (final BufferedReader reader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8)) {
            final String line = reader.readLine();
            if (line == null) {
                throw new EOFException("File ended prematurely.");
            }

            final ObjectMapper objectMapper = new ObjectMapper();
            JsonNode readNode = objectMapper.readTree(line);

            if (!(readNode instanceof ObjectNode)) {
                throw new IOException("Deserialized node " + readNode +
                    " is not an object node");
            }
            final ObjectNode dataObject = (ObjectNode) readNode;

            JsonNode dataVersionNode = dataObject.get(DATA_VERSION);
            if (dataVersionNode == null) {
                throw new IOException("Deserialized node " + readNode +
                    " does not have " + DATA_VERSION + " field");
            }

            if (dataVersionNode.asInt() != 0) {
                throw new UnsupportedVersionException("Unknown data version of " + dataVersionNode.toString());
            }

            final short dataVersion = dataVersionNode.shortValue();
            return QuorumStateDataJsonConverter.read(dataObject, dataVersion);
        } catch (IOException e) {
            throw new UncheckedIOException(
                String.format("Error while reading the Quorum status from the file %s", file), e);
        }
    }

    /**
     * Reads the election state from local file.
     */
    @Override
    public ElectionState readElectionState() {
        if (!stateFile.exists()) {
            return null;
        }

        QuorumStateData data = readStateFromFile(stateFile);

        return new ElectionState(data.leaderEpoch(),
            data.leaderId() == UNKNOWN_LEADER_ID ? OptionalInt.empty() :
                OptionalInt.of(data.leaderId()),
            data.votedId() == NOT_VOTED ? OptionalInt.empty() :
                OptionalInt.of(data.votedId()),
            data.currentVoters()
                .stream().map(Voter::voterId).collect(Collectors.toSet()));
    }

    @Override
    public void writeElectionState(ElectionState latest) {
        QuorumStateData data = new QuorumStateData()
            .setLeaderEpoch(latest.epoch)
            .setVotedId(latest.hasVoted() ? latest.votedId() : NOT_VOTED)
            .setLeaderId(latest.hasLeader() ? latest.leaderId() : UNKNOWN_LEADER_ID)
            .setCurrentVoters(voters(latest.voters()));
        writeElectionStateToFile(stateFile, data);
    }

    private List<Voter> voters(Set<Integer> votersId) {
        return votersId.stream().map(
            voterId -> new Voter().setVoterId(voterId)).collect(Collectors.toList());
    }

    private void writeElectionStateToFile(final File stateFile, QuorumStateData state) {
        final File temp = new File(stateFile.getAbsolutePath() + ".tmp");
        deleteFileIfExists(temp);

        log.trace("Writing tmp quorum state {}", temp.getAbsolutePath());

        try {
            try (final FileOutputStream fileOutputStream = new FileOutputStream(temp);
                 final BufferedWriter writer = new BufferedWriter(
                     new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8)
                 )
            ) {
                ObjectNode jsonState = (ObjectNode) QuorumStateDataJsonConverter.write(state, HIGHEST_SUPPORTED_VERSION);
                jsonState.set(DATA_VERSION, new ShortNode(HIGHEST_SUPPORTED_VERSION));
                writer.write(jsonState.toString());
                writer.flush();
                fileOutputStream.getFD().sync();
            }
            Utils.atomicMoveWithFallback(temp.toPath(), stateFile.toPath());
        } catch (IOException e) {
            throw new UncheckedIOException(
                String.format(
                    "Error while writing the Quorum status from the file %s",
                    stateFile.getAbsolutePath()
                ),
                e
            );
        } finally {
            // cleanup the temp file when the write finishes (either success or fail).
            deleteFileIfExists(temp);
        }
    }

    /**
     * Clear state store by deleting the local quorum state file
     */
    @Override
    public void clear() {
        deleteFileIfExists(stateFile);
        deleteFileIfExists(new File(stateFile.getAbsolutePath() + ".tmp"));
    }

    @Override
    public String toString() {
        return "Quorum state filepath: " + stateFile.getAbsolutePath();
    }

    private void deleteFileIfExists(File file) {
        try {
            Files.deleteIfExists(file.toPath());
        } catch (IOException e) {
            throw new UncheckedIOException(
                String.format("Error while deleting file %s", file.getAbsoluteFile()), e);
        }
    }
}
