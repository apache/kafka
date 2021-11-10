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

package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.CheckpointFile;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * This class represents a file containing the committed offsets of remote log metadata partitions.
 */
public class CommittedOffsetsFile {
    private static final int CURRENT_VERSION = 0;
    private static final String SEPARATOR = " ";

    private static final Pattern MINIMUM_ONE_WHITESPACE = Pattern.compile("\\s+");
    private final CheckpointFile<Map.Entry<Integer, Long>> checkpointFile;

    CommittedOffsetsFile(File offsetsFile) throws IOException {
        CheckpointFile.EntryFormatter<Map.Entry<Integer, Long>> formatter = new EntryFormatter();
        checkpointFile = new CheckpointFile<>(offsetsFile, CURRENT_VERSION, formatter);
    }

    private static class EntryFormatter implements CheckpointFile.EntryFormatter<Map.Entry<Integer, Long>> {

        @Override
        public String toString(Map.Entry<Integer, Long> entry) {
            // Each entry is stored in a new line as <partition-num offset>
            return entry.getKey() + SEPARATOR + entry.getValue();
        }

        @Override
        public Optional<Map.Entry<Integer, Long>> fromString(String line) {
            String[] strings = MINIMUM_ONE_WHITESPACE.split(line);
            if (strings.length != 2) {
                return Optional.empty();
            }

            try {
                return Optional.of(Utils.mkEntry(Integer.parseInt(strings[0]), Long.parseLong(strings[1])));
            } catch (NumberFormatException e) {
                return Optional.empty();
            }

        }
    }

    public synchronized void writeEntries(Map<Integer, Long> committedOffsets) throws IOException {
        checkpointFile.write(committedOffsets.entrySet());
    }

    public synchronized Map<Integer, Long> readEntries() throws IOException {
        List<Map.Entry<Integer, Long>> entries = checkpointFile.read();
        Map<Integer, Long> partitionToOffsets = new HashMap<>(entries.size());
        for (Map.Entry<Integer, Long> entry : entries) {
            Long existingValue = partitionToOffsets.put(entry.getKey(), entry.getValue());
            if (existingValue != null) {
                throw new IOException("Multiple entries exist for key: " + entry.getKey());
            }
        }

        return partitionToOffsets;
    }
}