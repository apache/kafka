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
package org.apache.kafka.streams.state.internals.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.BlockBasedTableConfigWithAccessibleCache;
import org.apache.kafka.streams.state.internals.RocksDBStore;
import org.apache.kafka.streams.state.internals.RocksDBTimestampedStore;
import org.apache.kafka.test.MockInternalProcessorContext;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.STATE_STORE_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.STORE_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TASK_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.THREAD_ID_TAG;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RocksDBBlockCacheMetricsTest {

    private static final String STORE_NAME = "test";
    private static final String METRICS_SCOPE = "test-scope";

    private static final TaskId TASK_ID = new TaskId(0, 0);

    public static Stream<Arguments> stores() {
        final File stateDir = TestUtils.tempDirectory("state");
        return Stream.of(
            Arguments.of(new RocksDBStore(STORE_NAME, METRICS_SCOPE), new MockInternalProcessorContext(new Properties(), TASK_ID, stateDir)),
            Arguments.of(new RocksDBTimestampedStore(STORE_NAME, METRICS_SCOPE), new MockInternalProcessorContext(new Properties(), TASK_ID, stateDir))
        );
    }

    static void withStore(final RocksDBStore store, final StateStoreContext context, final Runnable function) {
        store.init(context, store);
        try {
            function.run();
        } finally {
            store.close();
            try {
                Utils.delete(context.stateDir());
            } catch (final IOException e) {
                // ignore
            }
        }
    }

    @ParameterizedTest
    @MethodSource("stores")
    public void shouldRecordCorrectBlockCacheCapacity(final RocksDBStore store, final StateStoreContext ctx) {
        withStore(store, ctx, () ->
                assertMetric(ctx, STATE_STORE_LEVEL_GROUP, RocksDBMetrics.CAPACITY_OF_BLOCK_CACHE, BigInteger.valueOf(50 * 1024 * 1024L)));
    }

    @ParameterizedTest
    @MethodSource("stores")
    public void shouldRecordCorrectBlockCacheUsage(final RocksDBStore store, final StateStoreContext ctx) {
        withStore(store, ctx, () -> {
            final BlockBasedTableConfigWithAccessibleCache tableFormatConfig = (BlockBasedTableConfigWithAccessibleCache) store.getOptions().tableFormatConfig();
            final long usage = tableFormatConfig.blockCache().getUsage();
            assertMetric(ctx, STATE_STORE_LEVEL_GROUP, RocksDBMetrics.USAGE_OF_BLOCK_CACHE, BigInteger.valueOf(usage));
        });
    }

    @ParameterizedTest
    @MethodSource("stores")
    public void shouldRecordCorrectBlockCachePinnedUsage(final RocksDBStore store, final StateStoreContext ctx) {
        withStore(store, ctx, () -> {
            final BlockBasedTableConfigWithAccessibleCache tableFormatConfig = (BlockBasedTableConfigWithAccessibleCache) store.getOptions().tableFormatConfig();
            final long usage = tableFormatConfig.blockCache().getPinnedUsage();
            assertMetric(ctx, STATE_STORE_LEVEL_GROUP, RocksDBMetrics.PINNED_USAGE_OF_BLOCK_CACHE, BigInteger.valueOf(usage));
        });
    }

    public <T> void assertMetric(final StateStoreContext context, final String group, final String metricName, final T expected) {
        final StreamsMetricsImpl metrics = ProcessorContextUtils.metricsImpl(context);
        final MetricName name = metrics.metricsRegistry().metricName(
                metricName,
                group,
                "Ignored",
                storeLevelTagMap(TASK_ID.toString(), METRICS_SCOPE, STORE_NAME)
        );
        final KafkaMetric metric = (KafkaMetric) metrics.metrics().get(name);
        assertEquals(expected, metric.metricValue(), String.format("Value for metric '%s-%s' was incorrect", group, metricName));
    }

    public Map<String, String> threadLevelTagMap(final String threadId) {
        final Map<String, String> tagMap = new LinkedHashMap<>();
        tagMap.put(THREAD_ID_TAG, threadId);
        return tagMap;
    }

    public Map<String, String> taskLevelTagMap(final String threadId, final String taskId) {
        final Map<String, String> tagMap = threadLevelTagMap(threadId);
        tagMap.put(TASK_ID_TAG, taskId);
        return tagMap;
    }

    public Map<String, String> storeLevelTagMap(final String taskName,
                                                final String storeType,
                                                final String storeName) {
        final Map<String, String> tagMap = taskLevelTagMap(Thread.currentThread().getName(), taskName);
        tagMap.put(storeType + "-" + STORE_ID_TAG, storeName);
        return tagMap;
    }
}
