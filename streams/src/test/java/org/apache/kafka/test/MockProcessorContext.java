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

package org.apache.kafka.test;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.state.StateSerdes;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockProcessorContext implements ProcessorContext, RecordCollector.Supplier {

    private final KStreamTestDriver driver;
    private final Serde<?> keySerde;
    private final Serde<?> valSerde;
    private final RecordCollector.Supplier recordCollectorSupplier;
    private final File stateDir;

    private Map<String, StateStore> storeMap = new HashMap<>();
    private Map<String, StateRestoreCallback> restoreFuncs = new HashMap<>();

    long timestamp = -1L;

    public MockProcessorContext(StateSerdes<?, ?> serdes, RecordCollector collector) {
        this(null, null, serdes.keySerde(), serdes.valueSerde(), collector);
    }

    public MockProcessorContext(KStreamTestDriver driver, File stateDir,
                                Serde<?> keySerde,
                                Serde<?> valSerde,
                                final RecordCollector collector) {
        this(driver, stateDir, keySerde, valSerde,
                new RecordCollector.Supplier() {
                    @Override
                    public RecordCollector recordCollector() {
                        return collector;
                    }
                });
    }

    public MockProcessorContext(KStreamTestDriver driver, File stateDir,
                                Serde<?> keySerde,
                                Serde<?> valSerde,
                                RecordCollector.Supplier collectorSupplier) {
        this.driver = driver;
        this.stateDir = stateDir;
        this.keySerde = keySerde;
        this.valSerde = valSerde;
        this.recordCollectorSupplier = collectorSupplier;
    }

    @Override
    public RecordCollector recordCollector() {
        RecordCollector recordCollector = recordCollectorSupplier.recordCollector();

        if (recordCollector == null) {
            throw new UnsupportedOperationException("No RecordCollector specified");
        }
        return recordCollector;
    }

    public void setTime(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public TaskId taskId() {
        return new TaskId(0, 0);
    }

    @Override
    public String applicationId() {
        return "mockApplication";
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
        if (stateDir == null)
            throw new UnsupportedOperationException("State directory not specified");

        return stateDir;
    }

    @Override
    public StreamsMetrics metrics() {
        return new StreamsMetrics() {
            @Override
            public Sensor addLatencySensor(String scopeName, String entityName, String operationName, String... tags) {
                return null;
            }
            @Override
            public void recordLatency(Sensor sensor, long startNs, long endNs) {
            }
        };
    }

    @Override
    public void register(StateStore store, boolean loggingEnabled, StateRestoreCallback func) {
        storeMap.put(store.name(), store);
        restoreFuncs.put(store.name(), func);
    }

    @Override
    public StateStore getStateStore(String name) {
        return storeMap.get(name);
    }

    @Override
    public void schedule(long interval) {
        throw new UnsupportedOperationException("schedule() not supported.");
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> void forward(K key, V value) {
        driver.forward(key, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> void forward(K key, V value, int childIndex) {
        driver.forward(key, value, childIndex);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> void forward(K key, V value, String childName) {
        driver.forward(key, value, childName);
    }

    @Override
    public void commit() {
        throw new UnsupportedOperationException("commit() not supported.");
    }

    @Override
    public String topic() {
        return null;
    }

    @Override
    public int partition() {
        return -1;
    }

    @Override
    public long offset() {
        return -1L;
    }

    @Override
    public long timestamp() {
        return this.timestamp;
    }

    public Map<String, StateStore> allStateStores() {
        return Collections.unmodifiableMap(storeMap);
    }

    public void restore(String storeName, List<KeyValue<byte[], byte[]>> changeLog) {
        StateRestoreCallback restoreCallback = restoreFuncs.get(storeName);
        for (KeyValue<byte[], byte[]> entry : changeLog) {
            restoreCallback.restore(entry.key, entry.value);
        }
    }
}
