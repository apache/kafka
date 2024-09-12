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
package org.apache.kafka.test;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.CommitCallback;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.GlobalStateManager;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.Task.TaskType;

import java.io.File;
import java.util.Map;
import java.util.Set;

public class GlobalStateManagerStub implements GlobalStateManager {

    private final Set<String> storeNames;
    private final Map<TopicPartition, Long> offsets;
    private final File baseDirectory;
    public boolean initialized;
    public boolean closed;
    public boolean flushed;
    public boolean checkpointWritten;

    public GlobalStateManagerStub(final Set<String> storeNames,
                                  final Map<TopicPartition, Long> offsets,
                                  final File baseDirectory) {
        this.storeNames = storeNames;
        this.offsets = offsets;
        this.baseDirectory = baseDirectory;
    }

    @Override
    public void setGlobalProcessorContext(final InternalProcessorContext processorContext) {}

    @Override
    public Set<String> initialize() {
        initialized = true;
        return storeNames;
    }

    @Override
    public File baseDir() {
        return baseDirectory;
    }

    @Override
    public void registerStore(final StateStore store,
                              final StateRestoreCallback stateRestoreCallback,
                              final CommitCallback checkpoint) {}

    @Override
    public void flush() {
        flushed = true;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public void updateChangelogOffsets(final Map<TopicPartition, Long> writtenOffsets) {
        this.offsets.putAll(writtenOffsets);
    }

    @Override
    public void checkpoint() {
        checkpointWritten = true;
    }

    @Override
    public StateStore store(final String name) {
        return null;
    }

    @Override
    public StateStore globalStore(final String name) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> changelogOffsets() {
        return offsets;
    }

    @Override
    public TaskType taskType() {
        return TaskType.GLOBAL;
    }

    @Override
    public String changelogFor(final String storeName) {
        return null;
    }
}
