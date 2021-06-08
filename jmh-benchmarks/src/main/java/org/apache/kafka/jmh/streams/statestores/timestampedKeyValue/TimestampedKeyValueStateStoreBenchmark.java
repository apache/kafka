package org.apache.kafka.jmh.streams.statestores.timestampedKeyValue;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.openjdk.jmh.annotations.Benchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

public class TimestampedKeyValueStateStoreBenchmark {

    /*

    Things to test ->

    1) Serdes(serialisation/deserialisation)
    2) State stores- find all stores and add relevant benchmarks.
    3) various operators tests.
    4) e2e tests.
    5) DSL v/s PAPI.
    6) testing for joins.
    7) eos

    5 types of stores => kv, timestamped, windowed, timestampedWindowed, session

     */

    protected final int DISTINCT_KEYS = 1_000_000;

    protected final String[] keys = new String[DISTINCT_KEYS];

    protected final ValueAndTimestamp<String>[] values = new ValueAndTimestamp[DISTINCT_KEYS];

    protected final String KEY = "some_key";

    protected final String VALUE = "some_value";

    private final String SCAN_KEY = "scan_key";

    protected TimestampedKeyValueStore<String, String> timestampedKeyValueStore;

    private final long timestamp = new Date().getTime();

    protected void storeKeys() {
        for (int i = 0; i < DISTINCT_KEYS; ++i) {
            keys[i] = KEY + i;
            values[i] = ValueAndTimestamp.make(VALUE + i, timestamp);
        }
    }

    protected void storeScanKeys() {
        for (int i = 0; i < DISTINCT_KEYS; ++i) {
            timestampedKeyValueStore.put(SCAN_KEY + i, ValueAndTimestamp.make(VALUE + i, timestamp));
        }
    }

    protected ProcessorContext setupProcessorContext() {
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
        return context;
    }

    @Benchmark
    protected ValueAndTimestamp<String> testPersistentPutGetPerformance() {
        int counter = 0;
        counter++;
        final int index = counter % DISTINCT_KEYS;
        final String key = keys[index];
        timestampedKeyValueStore.put(key, values[index]);
        return timestampedKeyValueStore.get(key);
    }

    @Benchmark
    protected int testPersistentPrefixScanPerformance() {
        int numKeys = 0;
        final KeyValueIterator<String, ValueAndTimestamp<String>> prefixScan = timestampedKeyValueStore.prefixScan(SCAN_KEY, new StringSerializer());
        while (prefixScan.hasNext()) {
            prefixScan.next();
            numKeys++;
        }
        return numKeys;
    }

    @Benchmark
    protected int testPersistentRangeQueryPerformance() {
        int numKeys = 0;
        final KeyValueIterator<String, ValueAndTimestamp<String>> rangeScan = timestampedKeyValueStore.range(SCAN_KEY + 0, SCAN_KEY + (DISTINCT_KEYS - 1));
        while (rangeScan.hasNext()) {
            rangeScan.next();
            numKeys++;
        }
        return numKeys;
    }

    @Benchmark
    protected int testPersistentReverseRangeQueryPerformance() {
        int numKeys = 0;
        final KeyValueIterator<String, ValueAndTimestamp<String>> reverseRangeScan = timestampedKeyValueStore.reverseRange(SCAN_KEY + (DISTINCT_KEYS - 1), SCAN_KEY + 0);
        while (reverseRangeScan.hasNext()) {
            reverseRangeScan.next();
            numKeys++;
        }
        return numKeys;
    }

    @Benchmark
    protected int testPersistentAllPerformance() {
        int numKeys = 0;
        final KeyValueIterator<String, ValueAndTimestamp<String>> allScan = timestampedKeyValueStore.all();
        while (allScan.hasNext()) {
            allScan.next();
            numKeys++;
        }
        return numKeys;
    }

    @Benchmark
    protected List<KeyValue<String, ValueAndTimestamp<String>>> testPersistentPutAllPerformance() {
        List<KeyValue<String, ValueAndTimestamp<String>>> entries = new ArrayList<>();
        for (int count = 0; count < DISTINCT_KEYS; count++) {
            final int index = count % DISTINCT_KEYS;
            entries.add(KeyValue.pair(keys[index], values[index]));
        }
        timestampedKeyValueStore.putAll(entries);
        return entries;
    }

}
