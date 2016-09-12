/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.io.File;
import java.util.List;
import java.util.Map;

public class ProcessorContextImpl implements InternalProcessorContext, RecordCollector.Supplier {

    public static final String NONEXIST_TOPIC = "__null_topic__";

    private final TaskId id;
    private final StreamTask task;
    private final StreamsMetrics metrics;
    private final RecordCollector collector;
    private final ProcessorStateManager stateMgr;

    private final StreamsConfig config;
    private final Serde<?> keySerde;
    private final Serde<?> valSerde;
    private final ThreadCache cache;
    private boolean initialized;
    private RecordContext recordContext;
    private ProcessorNode currentNode;

    @SuppressWarnings("unchecked")
    public ProcessorContextImpl(TaskId id,
                                StreamTask task,
                                StreamsConfig config,
                                RecordCollector collector,
                                ProcessorStateManager stateMgr,
                                StreamsMetrics metrics,
                                final ThreadCache cache) {
        this.id = id;
        this.task = task;
        this.metrics = metrics;
        this.collector = collector;
        this.stateMgr = stateMgr;

        this.config = config;
        this.keySerde = config.keySerde();
        this.valSerde = config.valueSerde();
        this.cache = cache;
        this.initialized = false;
    }

    public void initialized() {
        this.initialized = true;
    }

    public ProcessorStateManager getStateMgr() {
        return stateMgr;
    }

    @Override
    public TaskId taskId() {
        return id;
    }

    @Override
    public String applicationId() {
        return task.applicationId();
    }

    @Override
    public RecordCollector recordCollector() {
        return this.collector;
    }

    @Override
    public Serde<?> keySerde() {
        return this.keySerde;
    }

    @Override
    public Serde<?> valueSerde() {
        return this.valSerde;
    }

    @Override
    public File stateDir() {
        return stateMgr.baseDir();
    }

    @Override
    public StreamsMetrics metrics() {
        return metrics;
    }

    /**
     * @throws IllegalStateException if this method is called before {@link #initialized()}
     */
    @Override
    public void register(StateStore store, boolean loggingEnabled, StateRestoreCallback stateRestoreCallback) {
        if (initialized)
            throw new IllegalStateException("Can only create state stores during initialization.");

        stateMgr.register(store, loggingEnabled, stateRestoreCallback);
    }

    /**
     * @throws TopologyBuilderException if an attempt is made to access this state store from an unknown node
     */
    @Override
    public StateStore getStateStore(String name) {
        ProcessorNode node = task.node();

        if (node == null)
            throw new TopologyBuilderException("Accessing from an unknown node");

        // TODO: restore this once we fix the ValueGetter initialization issue
        //if (!node.stateStores.contains(name))
        //    throw new TopologyBuilderException("Processor " + node.name() + " has no access to StateStore " + name);

        return stateMgr.getStore(name);
    }

    @Override
    public ThreadCache getCache() {
        return cache;
    }

    /**
     * @throws IllegalStateException if the task's record is null
     */
    @Override
    public String topic() {
        if (recordContext == null)
            throw new IllegalStateException("This should not happen as topic() should only be called while a record is processed");

        String topic = recordContext.topic();

        if (topic.equals(NONEXIST_TOPIC))
            return null;
        else
            return topic;
    }

    /**
     * @throws IllegalStateException if partition is null
     */
    @Override
    public int partition() {
        if (recordContext == null)
            throw new IllegalStateException("This should not happen as partition() should only be called while a record is processed");

        return recordContext.partition();
    }

    /**
     * @throws IllegalStateException if offset is null
     */
    @Override
    public long offset() {
        if (recordContext == null)
            throw new IllegalStateException("This should not happen as offset() should only be called while a record is processed");

        return recordContext.offset();
    }

    /**
     * @throws IllegalStateException if timestamp is null
     */
    @Override
    public long timestamp() {
        if (recordContext == null)
            throw new IllegalStateException("This should not happen as timestamp() should only be called while a record is processed");

        return recordContext.timestamp();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void forward(K key, V value) {
        ProcessorNode previousNode = currentNode;
        try {
            for (ProcessorNode child : (List<ProcessorNode>) currentNode.children()) {
                currentNode = child;
                child.process(key, value);
            }
        } finally {
            currentNode = previousNode;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void forward(K key, V value, int childIndex) {
        ProcessorNode previousNode = currentNode;
        final ProcessorNode child = (ProcessorNode<K, V>) currentNode.children().get(childIndex);
        currentNode = child;
        try {
            child.process(key, value);
        } finally {
            currentNode = previousNode;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void forward(K key, V value, String childName) {
        for (ProcessorNode child : (List<ProcessorNode<K, V>>) currentNode.children()) {
            if (child.name().equals(childName)) {
                ProcessorNode previousNode = currentNode;
                currentNode = child;
                try {
                    child.process(key, value);
                    return;
                } finally {
                    currentNode = previousNode;
                }
            }
        }
    }

    @Override
    public void commit() {
        task.needCommit();
    }

    @Override
    public void schedule(long interval) {
        task.schedule(interval);
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
}
