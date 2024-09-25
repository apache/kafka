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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.CommitCallback;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.Task.TaskType;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public interface StateManager {
    File baseDir();

    /**
     * @throws IllegalArgumentException if the store name has already been registered or if it is not a valid name
     * (e.g., when it conflicts with the names of internal topics, like the checkpoint file name)
     * @throws StreamsException if the store's change log does not contain the partition
     */
    void registerStore(final StateStore store,
                       final StateRestoreCallback stateRestoreCallback,
                       final CommitCallback checkpoint);

    StateStore store(final String name);

    void flush();

    void updateChangelogOffsets(final Map<TopicPartition, Long> writtenOffsets);

    void checkpoint();

    Map<TopicPartition, Long> changelogOffsets();

    void close() throws IOException;

    TaskType taskType();

    String changelogFor(final String storeName);

    // TODO: we can remove this when consolidating global state manager into processor state manager
    StateStore globalStore(final String name);
}
