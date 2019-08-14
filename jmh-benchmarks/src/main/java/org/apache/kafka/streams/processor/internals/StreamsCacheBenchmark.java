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

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.CachingKeyValueStore;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

/**
 * This is a simple example of a JMH benchmark.
 *
 * The sample code provided by the JMH project is a great place to start learning how to write correct benchmarks:
 * http://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/
 */
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class StreamsCacheBenchmark {

    private static final int DISTINCT_KEYS = 10_000;

    private static final String KEY = "the_key_to_use";

    private static final String VALUE = "the quick brown fox jumped over the lazy dog the olympics are about to start";

    private final Bytes[] keys = new Bytes[DISTINCT_KEYS];

    private final byte[][] values = new byte[DISTINCT_KEYS][];


    private int counter;

    private CachingKeyValueStore store;

    @Setup(Level.Trial)
    public void setUp() {
        for (int i = 0; i < DISTINCT_KEYS; ++i) {
            keys[i] = Bytes.wrap((KEY + i).getBytes(StandardCharsets.UTF_8));
            values[i] = (VALUE + i).getBytes(StandardCharsets.UTF_8);
        }

        store = new CachingKeyValueStore(new NotUsedKeyValueStore());
        final StreamsMetricsImpl metrics = new StreamsMetricsImpl(new Metrics(), "test-metrics");
        final ThreadCache cache = new ThreadCache(new LogContext("testCache "), 1_000_000_000, metrics);
        final StreamsConfig config = new StreamsConfig(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "test"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test")
        ));
        final ProcessorContextImpl context = new ProcessorContextImpl(new TaskId(0, 0), null, config, null, null, metrics, cache);
        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "topic", new RecordHeaders()));

        store.init(context, null);
    }

    @Benchmark
    public byte[] testCachePerformance() {
        counter++;
        final int index = counter % DISTINCT_KEYS;
        final Bytes key = keys[index];
        store.put(key, values[index]);

        return store.get(key);
    }

    public static void main(final String[] args) {
        final StreamsCacheBenchmark streamsCacheBenchmark = new StreamsCacheBenchmark();
        streamsCacheBenchmark.setUp();
        System.out.println(new String(streamsCacheBenchmark.testCachePerformance(), StandardCharsets.UTF_8));
    }

}
