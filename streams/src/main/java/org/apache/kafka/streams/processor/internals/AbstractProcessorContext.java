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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.io.File;
import java.util.Map;
import java.util.Objects;


public abstract class AbstractProcessorContext implements InternalProcessorContext {

    static final String NONEXIST_TOPIC = "__null_topic__";
    private final TaskId taskId;
    private final String applicationId;
    private final StreamsConfig config;
    private final StreamsMetrics metrics;
    private final Serde keySerde;
    private final ThreadCache cache;
    private final Serde valueSerde;
    private boolean initialized;
    private RecordContext recordContext;
    private ProcessorNode currentNode;
    final StateManager stateManager;

    public AbstractProcessorContext(final TaskId taskId,
                             final String applicationId,
                             final StreamsConfig config,
                             final StreamsMetrics metrics,
                             final StateManager stateManager,
                             final ThreadCache cache) {

        this.taskId = taskId;
        this.applicationId = applicationId;
        this.config = config;
        this.metrics = metrics;
        this.stateManager = stateManager;
        valueSerde = config.valueSerde();
        keySerde = config.keySerde();
        this.cache = cache;
    }

    @Override
    public String applicationId() {
        return applicationId;
    }

    @Override
    public TaskId taskId() {
        return taskId;
    }

    @Override
    public Serde<?> keySerde() {
        return keySerde;
    }

    @Override
    public Serde<?> valueSerde() {
        return valueSerde;
    }

    @Override
    public File stateDir() {
        return stateManager.baseDir();
    }

    @Override
    public StreamsMetrics metrics() {
        return metrics;
    }

    @Override
    public void register(final StateStore store, final boolean loggingEnabled, final StateRestoreCallback stateRestoreCallback) {
        if (initialized) {
            throw new IllegalStateException("Can only create state stores during initialization.");
        }
        Objects.requireNonNull(store, "store must not be null");
        stateManager.register(store, loggingEnabled, stateRestoreCallback);
    }

    /**
     * @throws IllegalStateException if the task's record is null
     */
    @Override
    public String topic() {
        if (recordContext == null) {
            throw new IllegalStateException("This should not happen as topic() should only be called while a record is processed");
        }

        final String topic = recordContext.topic();

        if (topic.equals(NONEXIST_TOPIC)) {
            return null;
        }

        return topic;
    }

    /**
     * @throws IllegalStateException if partition is null
     */
    @Override
    public int partition() {
        if (recordContext == null) {
            throw new IllegalStateException("This should not happen as partition() should only be called while a record is processed");
        }

        return recordContext.partition();
    }

    /**
     * @throws IllegalStateException if offset is null
     */
    @Override
    public long offset() {
        if (recordContext == null) {
            throw new IllegalStateException("This should not happen as offset() should only be called while a record is processed");
        }

        return recordContext.offset();
    }

    /**
     * @throws IllegalStateException if timestamp is null
     */
    @Override
    public long timestamp() {
        if (recordContext == null) {
            throw new IllegalStateException("This should not happen as timestamp() should only be called while a record is processed");
        }

        return recordContext.timestamp();
    }

    @Override
    public Map<String, Object> appConfigs() {
        return config.originals();
    }

    @Override
    public Map<String, Object> appConfigsWithPrefix(String prefix) {
        return config.originalsWithPrefix(prefix);
    }

    @Override
    public void setRecordContext(final RecordContext recordContext) {
        this.recordContext = recordContext;
    }

    @Override
    public RecordContext recordContext() {
        return this.recordContext;
    }

    @Override
    public void setCurrentNode(final ProcessorNode currentNode) {
        this.currentNode = currentNode;
    }

    @Override
    public ProcessorNode currentNode() {
        return currentNode;
    }

    @Override
    public ThreadCache getCache() {
        return cache;
    }

    @Override
    public void initialized() {
        initialized = true;
    }
}
