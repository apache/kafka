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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamConfig;
import org.apache.kafka.streams.StreamMetrics;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.TaskId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class ProcessorContextImpl implements ProcessorContext, RecordCollector.Supplier {

    private static final Logger log = LoggerFactory.getLogger(ProcessorContextImpl.class);

    private final TaskId id;
    private final StreamTask task;
    private final StreamMetrics metrics;
    private final RecordCollector collector;
    private final ProcessorStateManager stateMgr;

    private final Serializer<?> keySerializer;
    private final Serializer<?> valSerializer;
    private final Deserializer<?> keyDeserializer;
    private final Deserializer<?> valDeserializer;

    private boolean initialized;

    @SuppressWarnings("unchecked")
    public ProcessorContextImpl(TaskId id,
                                StreamTask task,
                                StreamConfig config,
                                RecordCollector collector,
                                ProcessorStateManager stateMgr,
                                StreamMetrics metrics) {
        this.id = id;
        this.task = task;
        this.metrics = metrics;
        this.collector = collector;
        this.stateMgr = stateMgr;

        this.keySerializer = config.keySerializer();
        this.valSerializer = config.valueSerializer();
        this.keyDeserializer = config.keyDeserializer();
        this.valDeserializer = config.valueDeserializer();

        this.initialized = false;
    }

    public void initialized() {
        this.initialized = true;
    }

    public TaskId id() {
        return id;
    }

    public ProcessorStateManager getStateMgr() {
        return stateMgr;
    }

    @Override
    public RecordCollector recordCollector() {
        return this.collector;
    }

    @Override
    public Serializer<?> keySerializer() {
        return this.keySerializer;
    }

    @Override
    public Serializer<?> valueSerializer() {
        return this.valSerializer;
    }

    @Override
    public Deserializer<?> keyDeserializer() {
        return this.keyDeserializer;
    }

    @Override
    public Deserializer<?> valueDeserializer() {
        return this.valDeserializer;
    }

    @Override
    public File stateDir() {
        return stateMgr.baseDir();
    }

    @Override
    public StreamMetrics metrics() {
        return metrics;
    }

    @Override
    public void register(StateStore store, boolean loggingEnabled, StateRestoreCallback stateRestoreCallback) {
        if (initialized)
            throw new KafkaException("Can only create state stores during initialization.");

        stateMgr.register(store, loggingEnabled, stateRestoreCallback);
    }

    @Override
    public StateStore getStateStore(String name) {
        ProcessorNode node = task.node();

        if (node == null)
            throw new KafkaException("accessing from an unknown node");

        if (!node.stateStores.contains(name))
            throw new KafkaException("Processor " + node.name() + " has no access to StateStore " + name);

        return stateMgr.getStore(name);
    }

    @Override
    public String topic() {
        if (task.record() == null)
            throw new IllegalStateException("this should not happen as topic() should only be called while a record is processed");

        return task.record().topic();
    }

    @Override
    public int partition() {
        if (task.record() == null)
            throw new IllegalStateException("this should not happen as partition() should only be called while a record is processed");

        return task.record().partition();
    }

    @Override
    public long offset() {
        if (this.task.record() == null)
            throw new IllegalStateException("this should not happen as offset() should only be called while a record is processed");

        return this.task.record().offset();
    }

    @Override
    public long timestamp() {
        if (task.record() == null)
            throw new IllegalStateException("this should not happen as timestamp() should only be called while a record is processed");

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
    public void commit() {
        task.needCommit();
    }

    @Override
    public void schedule(long interval) {
        task.schedule(interval);
    }
}
