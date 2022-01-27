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
import org.apache.kafka.streams.processor.CommitCallback;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;

import java.io.File;
import java.util.Map;
import org.apache.kafka.streams.processor.internals.Task.TaskType;

public class StateManagerStub implements StateManager {

    @Override
    public File baseDir() {
        return null;
    }

    @Override
    public void registerStore(final StateStore store,
                              final StateRestoreCallback stateRestoreCallback,
                              final CommitCallback checkpoint) {}

    @Override
    public void flush() {}

    @Override
    public void close() {}

    @Override
    public StateStore getStore(final String name) {
        return null;
    }

    @Override
    public StateStore getGlobalStore(final String name) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> changelogOffsets() {
        return null;
    }

    @Override
    public void updateChangelogOffsets(final Map<TopicPartition, Long> writtenOffsets) {}

    @Override
    public void checkpoint() {}

    @Override
    public TaskType taskType() {
        return null;
    }

    @Override
    public String changelogFor(final String storeName) {
        return null;
    }
}
