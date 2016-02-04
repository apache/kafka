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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class StandbyContextImpl implements ProcessorContext, RecordCollector.Supplier {

    private static final Logger log = LoggerFactory.getLogger(StandbyContextImpl.class);

    private final TaskId id;
    private final String jobId;
    private final StreamsMetrics metrics;
    private final ProcessorStateManager stateMgr;

    private final Serializer<?> keySerializer;
    private final Serializer<?> valSerializer;
    private final Deserializer<?> keyDeserializer;
    private final Deserializer<?> valDeserializer;

    private boolean initialized;

    public StandbyContextImpl(TaskId id,
                              String jobId,
                              StreamsConfig config,
                              ProcessorStateManager stateMgr,
                              StreamsMetrics metrics) {
        this.id = id;
        this.jobId = jobId;
        this.metrics = metrics;
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

    public ProcessorStateManager getStateMgr() {
        return stateMgr;
    }

    @Override
    public TaskId taskId() {
        return id;
    }

    @Override
    public String jobId() {
        return jobId;
    }

    @Override
    public RecordCollector recordCollector() {
        throw new UnsupportedOperationException();
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
    public StreamsMetrics metrics() {
        return metrics;
    }

    @Override
    public void register(StateStore store, boolean loggingEnabled, StateRestoreCallback stateRestoreCallback) {
        if (initialized)
            throw new IllegalStateException("Can only create state stores during initialization.");

        stateMgr.register(store, loggingEnabled, stateRestoreCallback);
    }

    @Override
    public StateStore getStateStore(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String topic() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int partition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long offset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long timestamp() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <K, V> void forward(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <K, V> void forward(K key, V value, int childIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void schedule(long interval) {
        throw new UnsupportedOperationException();
    }
}
