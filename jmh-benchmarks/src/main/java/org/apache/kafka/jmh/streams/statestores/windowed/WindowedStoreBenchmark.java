package org.apache.kafka.jmh.streams.statestores.windowed;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.*;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.openjdk.jmh.annotations.Benchmark;

import java.time.Instant;
import java.util.*;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

public class WindowedStoreBenchmark {

    protected final int DISTINCT_KEYS = 1_000_000;

    protected final int WINDOW_SIZE = 1;

    private final int NUM_WINDOWS = 100;

    private final Map<String, List<ValueAndTimestamp<String>>> windowedValues = new HashMap<>();

    protected final String[] keys = new String[DISTINCT_KEYS];

    protected final String KEY = "some_key";

    protected final String VALUE = "some_value";

    protected WindowStore<String, String> windowStore;

    private final long timestamp = new Date().getTime();

    protected void generateWindowedKeys() {
        for (int i = 0; i < DISTINCT_KEYS; ++i) {
            keys[i] = KEY + i;
            List<ValueAndTimestamp<String>> valueAndTimestamps = new ArrayList<>();
            for (int w = 0; w < NUM_WINDOWS; w++) {
                valueAndTimestamps.add(ValueAndTimestamp.make(
                    VALUE + i,
                    Instant.ofEpochMilli(timestamp).
                        plusSeconds(60 * (w + WINDOW_SIZE)).
                        toEpochMilli())
                );
            }
            windowedValues.put(keys[i], valueAndTimestamps);
        }
    }

    protected void putWindowedKeys() {
        for (String windowedKey: windowedValues.keySet()) {
            List<ValueAndTimestamp<String>> valueAndTimestamps = windowedValues.get(windowedKey);
            for (ValueAndTimestamp<String> valueAndTimestamp: valueAndTimestamps)
            windowStore.put(windowedKey, valueAndTimestamp.value(), valueAndTimestamp.timestamp());
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
                new StateDirectory(config, Time.SYSTEM, true, false),
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
    protected int testFetchKeyAtTimePerformance() {
        List<String> records = new ArrayList<>();
        for (String key: keys) {
            for (int w = 0; w < NUM_WINDOWS; w++) {
                final String fetch = windowStore.fetch(key,
                    Instant.ofEpochMilli(timestamp).
                        plusSeconds(60 * (w + WINDOW_SIZE))
                    .toEpochMilli()
                );
                records.add(fetch);
            }
        }
        return records.size();
    }

    @Benchmark
    protected int testFetchKeyInTimeRangePerformance() {
        int records = 0;
        for (String key: keys) {
            for (int w = 0; w < NUM_WINDOWS; w++) {
                final WindowStoreIterator<String> fetch = windowStore.fetch(key,
                    Instant.ofEpochMilli(timestamp),
                    Instant.ofEpochMilli(timestamp).
                        plusSeconds(60 * (w + WINDOW_SIZE))
                );
                while (fetch.hasNext()) {
                    fetch.next();
                    records++;
                }
            }
        }
        return records;
    }

    @Benchmark
    protected int testFetchForKeyRangeInTimeRangePerformance() {
        int records = 0;
        final KeyValueIterator<Windowed<String>, String> fetch = windowStore.fetch(
            keys[0],
            keys[keys.length - 1],
            Instant.ofEpochMilli(timestamp),
            Instant.ofEpochMilli(timestamp).
                plusSeconds(60 * (NUM_WINDOWS + WINDOW_SIZE))
        );
        while (fetch.hasNext()) {
            fetch.next();
            records++;
        }
        return records;
    }

    @Benchmark
    protected int testFetchAllPerformance() {
        int records = 0;
        final KeyValueIterator<Windowed<String>, String> fetch = windowStore.fetchAll(
            Instant.ofEpochMilli(timestamp),
            Instant.ofEpochMilli(timestamp).
                plusSeconds(60 * (NUM_WINDOWS + WINDOW_SIZE))
        );
        while (fetch.hasNext()) {
            fetch.next();
            records++;
        }
        return records;
    }

    @Benchmark
    protected int testAllPerformance() {
        int records = 0;
        final KeyValueIterator<Windowed<String>, String> fetch = windowStore.all();
        while (fetch.hasNext()) {
            fetch.next();
            records++;
        }
        return records;
    }

    @Benchmark
    protected int testFetchKeyInBackwardTimeRangePerformance() {
        int records = 0;
        for (String key: keys) {
            for (int w = 0; w < NUM_WINDOWS; w++) {
                final WindowStoreIterator<String> fetch = windowStore.backwardFetch(key,
                    Instant.ofEpochMilli(timestamp),
                    Instant.ofEpochMilli(timestamp).
                        plusSeconds(60 * (w + WINDOW_SIZE))
                );
                while (fetch.hasNext()) {
                    fetch.next();
                    records++;
                }
            }
        }
        return records;
    }

    @Benchmark
    protected int testFetchForKeyRangeInBackwardTimeRangePerformance() {
        int records = 0;
        final KeyValueIterator<Windowed<String>, String> fetch = windowStore.backwardFetch(
            keys[0],
            keys[keys.length - 1],
            Instant.ofEpochMilli(timestamp),
            Instant.ofEpochMilli(timestamp).
                plusSeconds(60 * (NUM_WINDOWS + WINDOW_SIZE))
        );
        while (fetch.hasNext()) {
            fetch.next();
            records++;
        }
        return records;
    }

    @Benchmark
    protected int testBackwardFetchAllPerformance() {
        int records = 0;
        final KeyValueIterator<Windowed<String>, String> fetch = windowStore.backwardFetchAll(
            Instant.ofEpochMilli(timestamp),
            Instant.ofEpochMilli(timestamp).
                plusSeconds(60 * (NUM_WINDOWS + WINDOW_SIZE))
        );
        while (fetch.hasNext()) {
            fetch.next();
            records++;
        }
        return records;
    }

    @Benchmark
    protected int testBackwardAllPerformance() {
        int records = 0;
        final KeyValueIterator<Windowed<String>, String> fetch = windowStore.backwardAll();
        while (fetch.hasNext()) {
            fetch.next();
            records++;
        }
        return records;
    }
}
