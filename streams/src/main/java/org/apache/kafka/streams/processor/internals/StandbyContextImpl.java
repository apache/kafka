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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;

import java.io.File;

public class StandbyContextImpl implements ProcessorContext, RecordCollector.Supplier {

    private final TaskId id;
    private final String applicationId;
    private final StreamsMetrics metrics;
    private final ProcessorStateManager stateMgr;

    private final Serde<?> keySerde;
    private final Serde<?> valSerde;

    private boolean initialized;

    public StandbyContextImpl(TaskId id,
                              String applicationId,
                              StreamsConfig config,
                              ProcessorStateManager stateMgr,
                              StreamsMetrics metrics) {
        this.id = id;
        this.applicationId = applicationId;
        this.metrics = metrics;
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
        return applicationId;
    }

    @Override
    public RecordCollector recordCollector() {
        throw new UnsupportedOperationException();
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

    @Override
    public void register(StateStore store, boolean loggingEnabled, StateRestoreCallback stateRestoreCallback) {
        if (initialized)
            throw new IllegalStateException("Can only create state stores during initialization.");

        stateMgr.register(store, loggingEnabled, stateRestoreCallback);
    }

    @Override
    public StateStore getStateStore(String name) {
        throw new UnsupportedOperationException("getStateStore() not supported.");
    }

    @Override
    public String topic() {
        throw new UnsupportedOperationException("topic() not supported.");
    }

    @Override
    public int partition() {
        throw new UnsupportedOperationException("partition() not supported.");
    }

    @Override
    public long offset() {
        throw new UnsupportedOperationException("offset() not supported.");
    }

    @Override
    public long timestamp() {
        throw new UnsupportedOperationException("timestamp() not supported.");
    }

    @Override
    public <K, V> void forward(K key, V value) {
        throw new UnsupportedOperationException("forward() not supported.");
    }

    @Override
    public <K, V> void forward(K key, V value, int childIndex) {
        throw new UnsupportedOperationException("forward() not supported.");
    }

    @Override
    public void commit() {
        throw new UnsupportedOperationException("commit() not supported.");
    }

    @Override
    public void schedule(long interval) {
        throw new UnsupportedOperationException("schedule() not supported.");
    }
}
