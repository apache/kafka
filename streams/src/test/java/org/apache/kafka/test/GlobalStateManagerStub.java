/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.test;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.GlobalStateManager;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class GlobalStateManagerStub implements GlobalStateManager {

    private final Set<String> storeNames;
    private final Map<TopicPartition, Long> offsets;
    public boolean initialized;
    public boolean closed;

    public GlobalStateManagerStub(final Set<String> storeNames, final Map<TopicPartition, Long> offsets) {
        this.storeNames = storeNames;
        this.offsets = offsets;
    }

    @Override
    public Set<String> initialize(final InternalProcessorContext processorContext) {
        initialized = true;
        return storeNames;
    }
    
    @Override
    public File baseDir() {
        return null;
    }

    @Override
    public void register(final StateStore store, final boolean loggingEnabled, final StateRestoreCallback stateRestoreCallback) {

    }

    @Override
    public void flush(final InternalProcessorContext context) {

    }

    @Override
    public void close(final Map<TopicPartition, Long> offsets) throws IOException {
        this.offsets.putAll(offsets);
        closed = true;
    }

    @Override
    public StateStore getGlobalStore(final String name) {
        return null;
    }

    @Override
    public StateStore getStore(final String name) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> checkpointedOffsets() {
        return offsets;
    }
}
