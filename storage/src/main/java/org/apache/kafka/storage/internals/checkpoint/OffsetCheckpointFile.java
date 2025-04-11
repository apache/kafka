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
import org.apache.kafka.server.common.CheckpointFile;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

public class OffsetCheckpointFile {

    private static final Pattern WHITESPACES_PATTERN = Pattern.compile("\\s+");
    public static final int CURRENT_VERSION = 0;

    private final File file;
    private final CheckpointFileWithFailureHandler<TopicPartitionOffset> checkpoint;

    /**
     * This class persists a map of (Partition => Offsets) to a file (for a certain replica)
     * The format in the offset checkpoint file is like this:
     * <pre>
     *  -----checkpoint file begin------
     *  0                <- OffsetCheckpointFile.currentVersion
     *  2                <- following entries size
     *  tp1  par1  1     <- the format is: TOPIC  PARTITION  OFFSET
     *  tp1  par2  2
     *  -----checkpoint file end----------
     *  </pre>
     */
    public OffsetCheckpointFile(File file, LogDirFailureChannel logDirFailureChannel) throws IOException {
        this.file = file;
        checkpoint = new CheckpointFileWithFailureHandler<>(file, OffsetCheckpointFile.CURRENT_VERSION,
                new OffsetCheckpointFile.Formatter(), logDirFailureChannel, file.getParent());
    }

    public void write(Map<TopicPartition, Long> offsets) {
        List<TopicPartitionOffset> list = new ArrayList<>(offsets.size());
        offsets.forEach((key, value) -> list.add(new TopicPartitionOffset(key, value)));
        checkpoint.write(list);
    }

    // The returned Map must be mutable
    public Map<TopicPartition, Long> read() {
        List<TopicPartitionOffset> list = checkpoint.read();
        Map<TopicPartition, Long> result = new HashMap<>(list.size());
        list.forEach(tpo -> result.put(tpo.tp, tpo.offset));
        return result;
    }

    public File file() {
        return file;
    }

    static class Formatter implements CheckpointFile.EntryFormatter<TopicPartitionOffset> {

        @Override
        public String toString(TopicPartitionOffset tpo) {
            TopicPartition tp = tpo.tp;
            return tp.topic() + " " + tp.partition() + " " + tpo.offset;
        }

        @Override
        public Optional<TopicPartitionOffset> fromString(String line) {
            String[] parts = WHITESPACES_PATTERN.split(line);
            if (parts.length == 3) {
                return Optional.of(new TopicPartitionOffset(new TopicPartition(parts[0], Integer.parseInt(parts[1])), Long.parseLong(parts[2])));
            } else {
                return Optional.empty();
            }
        }
    }

    static class TopicPartitionOffset {

        final TopicPartition tp;
        final long offset;

        TopicPartitionOffset(TopicPartition tp, long offset) {
            this.tp = tp;
            this.offset = offset;
        }
    }

}
