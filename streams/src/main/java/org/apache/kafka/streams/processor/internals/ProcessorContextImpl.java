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
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.TaskId;

import java.io.File;
import java.util.Map;

public class ProcessorContextImpl implements ProcessorContext, RecordCollector.Supplier {

    public static final String NONEXIST_TOPIC = "__null_topic__";

    private final TaskId id;
    private final StreamTask task;
    private final StreamsMetrics metrics;
    private final RecordCollector collector;
    private final ProcessorStateManager stateMgr;

    private final StreamsConfig config;
    private final Serde<?> keySerde;
    private final Serde<?> valSerde;

    private boolean initialized;
    private Long timestamp;
    private String topic;
    private Long offset;
    private Integer partition;

    @SuppressWarnings("unchecked")
    public ProcessorContextImpl(TaskId id,
                                StreamTask task,
                                StreamsConfig config,
                                RecordCollector collector,
                                ProcessorStateManager stateMgr,
                                StreamsMetrics metrics) {
        this.id = id;
        this.task = task;
        this.metrics = metrics;
        this.collector = collector;
        this.stateMgr = stateMgr;

        this.config = config;
        this.keySerde = config.keySerde();
        this.valSerde = config.valueSerde();

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
    public synchronized String topic() {
        if (topic == null || topic.equals(NONEXIST_TOPIC))
            return null;
        else
            return topic;
    }

    /**
     * @throws IllegalStateException if partition is null
     */
    @Override
    public synchronized int partition() {
        if (partition == null) {
            throw new IllegalStateException("This should not happen as partition() should only be called while a record is processed");
        }
        return partition;
    }

    /**
     * @throws IllegalStateException if offset is null
     */
    @Override
    public synchronized long offset() {
        if (offset == null) {
            throw new IllegalStateException("This should not happen as offset() should only be called while a record is processed");
        }
        return offset;
    }

    /**
     * @throws IllegalStateException if timestamp is null
     */
    @Override
    public synchronized long timestamp() {
        if (timestamp == null) {
            throw new IllegalStateException("This should not happen as timestamp should be set during record processing");
        }
        return timestamp;
    }

    @Override
    public <K, V> void forward(K key, V value) {
        task.forward(key, value);
    }

    @Override
    public <K, V> void forward(K key, V value, int childIndex) {
        task.forward(key, value, childIndex);
    }

    @Override
    public <K, V> void forward(K key, V value, String childName) {
        task.forward(key, value, childName);
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

    public synchronized void update(final StampedRecord record) {
        this.timestamp = record.timestamp;
        this.partition = record.partition();
        this.offset = record.offset();
        this.topic = record.topic();
    }
}
