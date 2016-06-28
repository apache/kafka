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

public class ProcessorContextImpl implements ProcessorContext, RecordCollector.Supplier {

    public static final String NONEXIST_TOPIC = "__null_topic__";

    private final TaskId id;
    private final StreamTask task;
    private final StreamsMetrics metrics;
    private final RecordCollector collector;
    private final ProcessorStateManager stateMgr;

    private final Serde<?> keySerde;
    private final Serde<?> valSerde;

    private boolean initialized;

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

    /**
     * @throws IllegalStateException if the task's record is null
     */
    @Override
    public String topic() {
        if (task.record() == null)
            throw new IllegalStateException("This should not happen as topic() should only be called while a record is processed");

        String topic = task.record().topic();

        if (topic.equals(NONEXIST_TOPIC))
            return null;
        else
            return topic;
    }

    /**
     * @throws IllegalStateException if the task's record is null
     */
    @Override
    public int partition() {
        if (task.record() == null)
            throw new IllegalStateException("This should not happen as partition() should only be called while a record is processed");

        return task.record().partition();
    }

    /**
     * @throws IllegalStateException if the task's record is null
     */
    @Override
    public long offset() {
        if (this.task.record() == null)
            throw new IllegalStateException("This should not happen as offset() should only be called while a record is processed");

        return this.task.record().offset();
    }

    /**
     * @throws IllegalStateException if the task's record is null
     */
    @Override
    public long timestamp() {
        if (task.record() == null)
            throw new IllegalStateException("This should not happen as timestamp() should only be called while a record is processed");

        return task.record().timestamp;
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
}
