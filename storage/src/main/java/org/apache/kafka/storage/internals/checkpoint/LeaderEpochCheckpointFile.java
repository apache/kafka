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

import org.apache.kafka.server.common.CheckpointFile.EntryFormatter;
import org.apache.kafka.storage.internals.log.EpochEntry;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * This class persists a map of (LeaderEpoch => Offsets) to a file (for a certain replica)
 * <p>
 * The format in the LeaderEpoch checkpoint file is like this:
 * -----checkpoint file begin------
 * 0                <- LeaderEpochCheckpointFile.currentVersion
 * 2                <- following entries size
 * 0  1     <- the format is: leader_epoch(int32) start_offset(int64)
 * 1  2
 * -----checkpoint file end----------
 */
public class LeaderEpochCheckpointFile implements LeaderEpochCheckpoint {

    public static final Formatter FORMATTER = new Formatter();

    private static final String LEADER_EPOCH_CHECKPOINT_FILENAME = "leader-epoch-checkpoint";
    private static final Pattern WHITE_SPACES_PATTERN = Pattern.compile("\\s+");
    private static final int CURRENT_VERSION = 0;

    private final CheckpointFileWithFailureHandler<EpochEntry> checkpoint;

    public LeaderEpochCheckpointFile(File file, LogDirFailureChannel logDirFailureChannel) throws IOException {
        checkpoint = new CheckpointFileWithFailureHandler<>(file, CURRENT_VERSION, FORMATTER, logDirFailureChannel, file.getParentFile().getParent());
    }

    public void write(Collection<EpochEntry> epochs) {
        checkpoint.write(epochs);
    }

    public List<EpochEntry> read() {
        return checkpoint.read();
    }

    public static File newFile(File dir) {
        return new File(dir, LEADER_EPOCH_CHECKPOINT_FILENAME);
    }

    private static class Formatter implements EntryFormatter<EpochEntry> {

        public String toString(EpochEntry entry) {
            return entry.epoch + " " + entry.startOffset;
        }

        public Optional<EpochEntry> fromString(String line) {
            String[] strings = WHITE_SPACES_PATTERN.split(line);
            return (strings.length == 2) ? Optional.of(new EpochEntry(Integer.parseInt(strings[0]), Long.parseLong(strings[1]))) : Optional.empty();
        }
    }
}