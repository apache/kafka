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

package org.apache.kafka.jmh.streams.statestores;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.Level;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

@State(Scope.Thread)
@Fork(value = 1)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.Throughput)
public class StreamsPersistentStoreBenchmark {

    private int counter;
    private static final int DISTINCT_KEYS = 10_000;

    private static final String KEY = "some_key";

    private static final String SCAN_KEY = "scan_key";

    private static final String PUT_ALL_KEY = "put_all_key";

    private static final String VALUE = "some_value";

    private KeyValueStore<String, String> persistentKVStore;

    private final String[] keys = new String[DISTINCT_KEYS];

    private final String[] values = new String[DISTINCT_KEYS];

    @Setup(Level.Trial)
    public void setUp() {

        for (int i = 0; i < DISTINCT_KEYS; ++i) {
            keys[i] = KEY + i;
            values[i] = VALUE + i;
        }

        persistentKVStore = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("rocks"),
            Serdes.String(),
            Serdes.String())
            .withCachingDisabled()
            .withLoggingDisabled()
            .build();

        final StreamsMetricsImpl metrics = new StreamsMetricsImpl(new Metrics(),
            "test-metrics",
            StreamsConfig.METRICS_LATEST,
            Time.SYSTEM
        );
        final ThreadCache cache = new ThreadCache(new LogContext("testCache "), 1_000_000_000, metrics);
        final StreamsConfig config = new StreamsConfig(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "test"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test")
        ));
        final TaskId id = new TaskId(0, 0);
        final ProcessorStateManager stateMgr;
        try {
            stateMgr = new ProcessorStateManager(
                id,
                Task.TaskType.ACTIVE,
                false,
                new LogContext("jmh"),
                new StateDirectory(config, Time.SYSTEM, true),
                null,
                Collections.emptyMap(),
                Collections.emptySet()
            );
        } catch (final ProcessorStateException e) {
            throw new RuntimeException(e);
        }

        final ProcessorContextImpl context = new ProcessorContextImpl(
            id,
            config,
            stateMgr,
            metrics,
            cache
        );

        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "topic", new RecordHeaders()));
        persistentKVStore.init((StateStoreContext) context, persistentKVStore);

        for (int i = 0; i < DISTINCT_KEYS; ++i) {
            persistentKVStore.put(SCAN_KEY + i, VALUE + i);
        }

    }

    @Benchmark
    public String testPersistentPutGetPerformance() {
        counter++;
        final int index = counter % DISTINCT_KEYS;
        final String key = keys[index];
        persistentKVStore.put(key, values[index]);
        return persistentKVStore.get(key);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public int testPersistentPrefixScanPerformance() {
        int numKeys = 0;
        final KeyValueIterator<String, String> prefixScan = persistentKVStore.prefixScan(SCAN_KEY, new StringSerializer());
        while (prefixScan.hasNext()) {
            prefixScan.next();
            numKeys++;
        }
        return numKeys;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public int testPersistentRangeQueryPerformance() {
        int numKeys = 0;
        final KeyValueIterator<String, String> rangeScan = persistentKVStore.range(SCAN_KEY + 0, SCAN_KEY + (DISTINCT_KEYS - 1));
        while (rangeScan.hasNext()) {
            rangeScan.next();
            numKeys++;
        }
        return numKeys;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public int testPersistentReverseRangeQueryPerformance() {
        int numKeys = 0;
        final KeyValueIterator<String, String> reverseRangeScan = persistentKVStore.reverseRange(SCAN_KEY + (DISTINCT_KEYS - 1), SCAN_KEY + 0);
        while (reverseRangeScan.hasNext()) {
            reverseRangeScan.next();
            numKeys++;
        }
        return numKeys;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public int testPersistentAllPerformance() {
        int numKeys = 0;
        final KeyValueIterator<String, String> allScan = persistentKVStore.all();
        while (allScan.hasNext()) {
            allScan.next();
            numKeys++;
        }
        return numKeys;
    }

    @Benchmark
    public List<KeyValue<String, String>> testPersistentPutAllPerformance() {
        List<KeyValue<String, String>> entries = new ArrayList<>();
        for (int count = 0; count < DISTINCT_KEYS; count++) {
            final int index = count % DISTINCT_KEYS;
            entries.add(KeyValue.pair(keys[index], values[index]));
        }
        persistentKVStore.putAll(entries);
        return entries;
    }

}