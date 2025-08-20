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

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Loads checkpoint files on demand and caches the offsets for reuse.
 */
public class LazyOffsetCheckpoints implements OffsetCheckpoints {

    private final Map<String, LazyOffsetCheckpointMap> lazyCheckpointsByLogDir;

    public LazyOffsetCheckpoints(Map<String, OffsetCheckpointFile> checkpointsByLogDir) {
        lazyCheckpointsByLogDir = checkpointsByLogDir.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> new LazyOffsetCheckpointMap(entry.getValue())));
    }

    @Override
    public Optional<Long> fetch(String logDir, TopicPartition topicPartition) {
        LazyOffsetCheckpointMap offsetCheckpointFile = lazyCheckpointsByLogDir.get(logDir);
        if (offsetCheckpointFile == null) {
            throw new IllegalArgumentException("No checkpoint file for log dir " + logDir);
        }
        return offsetCheckpointFile.fetch(topicPartition);
    }

    static class LazyOffsetCheckpointMap {
        private Map<TopicPartition, Long> offsets;
        private final OffsetCheckpointFile checkpoint;

        LazyOffsetCheckpointMap(OffsetCheckpointFile checkpoint) {
            this.checkpoint = checkpoint;
        }

        synchronized Optional<Long> fetch(TopicPartition topicPartition) {
            if (offsets == null) {
                offsets = checkpoint.read();
            }
            return Optional.ofNullable(offsets.get(topicPartition));
        }
    }
}
