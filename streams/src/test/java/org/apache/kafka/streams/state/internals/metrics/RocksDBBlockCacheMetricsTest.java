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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.STATE_STORE_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.STORE_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TASK_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.THREAD_ID_TAG;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class RocksDBBlockCacheMetricsTest {

    private static final String STORE_NAME = "test";
    private static final String METRICS_SCOPE = "test-scope";

    @Parameterized.Parameters(name = "{0}")
    public static Object[] stores() {
        return new RocksDBStore[] {
            new RocksDBStore(STORE_NAME, METRICS_SCOPE),
            new RocksDBTimestampedStore(STORE_NAME, METRICS_SCOPE)
        };
    }

    private static TaskId taskId = new TaskId(0, 0);
    private static StateStoreContext context;

    @Parameterized.Parameter
    public RocksDBStore store;

    @Parameterized.BeforeParam
    public static void setUpStore(final RocksDBStore store) {
        context = new MockInternalProcessorContext(
                new Properties(),
                taskId,
                TestUtils.tempDirectory("state")
        );
        store.init(context, store);
    }

    @Parameterized.AfterParam
    public static void tearDownStore(final RocksDBStore store) throws IOException {
        store.close();
        Utils.delete(context.stateDir());
    }

    @Test
    public void shouldRecordCorrectBlockCacheCapacity() {
        assertMetric(STATE_STORE_LEVEL_GROUP, RocksDBMetrics.CAPACITY_OF_BLOCK_CACHE, BigInteger.valueOf(50 * 1024 * 1024L));
    }

    @Test
    public void shouldRecordCorrectBlockCacheUsage() {
        final BlockBasedTableConfigWithAccessibleCache tableFormatConfig = (BlockBasedTableConfigWithAccessibleCache) store.getOptions().tableFormatConfig();
        final long usage = tableFormatConfig.blockCache().getUsage();
        assertMetric(STATE_STORE_LEVEL_GROUP, RocksDBMetrics.USAGE_OF_BLOCK_CACHE, BigInteger.valueOf(usage));
    }

    @Test
    public void shouldRecordCorrectBlockCachePinnedUsage() {
        final BlockBasedTableConfigWithAccessibleCache tableFormatConfig = (BlockBasedTableConfigWithAccessibleCache) store.getOptions().tableFormatConfig();
        final long usage = tableFormatConfig.blockCache().getPinnedUsage();
        assertMetric(STATE_STORE_LEVEL_GROUP, RocksDBMetrics.PINNED_USAGE_OF_BLOCK_CACHE, BigInteger.valueOf(usage));
    }

    public <T> void assertMetric(final String group, final String metricName, final T expected) {
        final StreamsMetricsImpl metrics = ProcessorContextUtils.getMetricsImpl(context);
        final MetricName name = metrics.metricsRegistry().metricName(
                metricName,
                group,
                "Ignored",
                storeLevelTagMap(taskId.toString(), METRICS_SCOPE, STORE_NAME)
        );
        final KafkaMetric metric = (KafkaMetric) metrics.metrics().get(name);
        assertEquals(String.format("Value for metric '%s-%s' was incorrect", group, metricName), expected, metric.metricValue());
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
