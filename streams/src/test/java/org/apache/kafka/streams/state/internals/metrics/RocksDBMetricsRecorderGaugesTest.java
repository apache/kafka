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

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.junit.Test;
import org.rocksdb.Cache;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;

import java.math.BigInteger;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.STATE_STORE_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.STORE_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TASK_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.THREAD_ID_TAG;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.CAPACITY_OF_BLOCK_CACHE;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.COMPACTION_PENDING;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.CURRENT_SIZE_OF_ACTIVE_MEMTABLE;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.CURRENT_SIZE_OF_ALL_MEMTABLES;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.ESTIMATED_BYTES_OF_PENDING_COMPACTION;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.ESTIMATED_MEMORY_OF_TABLE_READERS;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.ESTIMATED_NUMBER_OF_KEYS;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.LIVE_SST_FILES_SIZE;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.MEMTABLE_FLUSH_PENDING;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.NUMBER_OF_DELETES_ACTIVE_MEMTABLE;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.NUMBER_OF_DELETES_IMMUTABLE_MEMTABLES;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.NUMBER_OF_ENTRIES_ACTIVE_MEMTABLE;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.NUMBER_OF_ENTRIES_IMMUTABLE_MEMTABLES;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.NUMBER_OF_IMMUTABLE_MEMTABLES;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.NUMBER_OF_RUNNING_COMPACTIONS;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.NUMBER_OF_RUNNING_FLUSHES;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.PINNED_USAGE_OF_BLOCK_CACHE;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.SIZE_OF_ALL_MEMTABLES;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.NUMBER_OF_BACKGROUND_ERRORS;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.TOTAL_SST_FILES_SIZE;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.USAGE_OF_BLOCK_CACHE;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.powermock.api.easymock.PowerMock.replay;

public class RocksDBMetricsRecorderGaugesTest {
    private static final String METRICS_SCOPE = "metrics-scope";
    private static final TaskId TASK_ID = new TaskId(0, 0);
    private static final String STORE_NAME = "store-name";
    private static final String SEGMENT_STORE_NAME_1 = "segment-store-name-1";
    private static final String SEGMENT_STORE_NAME_2 = "segment-store-name-2";
    private static final String ROCKSDB_PROPERTIES_PREFIX = "rocksdb.";

    private final RocksDB dbToAdd1 = mock(RocksDB.class);
    private final RocksDB dbToAdd2 = mock(RocksDB.class);
    private final Cache cacheToAdd1 = mock(Cache.class);
    private final Cache cacheToAdd2 = mock(Cache.class);
    private final Statistics statisticsToAdd1 = mock(Statistics.class);
    private final Statistics statisticsToAdd2 = mock(Statistics.class);

    @Test
    public void shouldGetNumberOfImmutableMemTables() throws Exception {
        runAndVerifySumOfProperties(NUMBER_OF_IMMUTABLE_MEMTABLES);
    }

    @Test
    public void shouldGetCurrentSizeofActiveMemTable() throws Exception {
        runAndVerifySumOfProperties(CURRENT_SIZE_OF_ACTIVE_MEMTABLE);
    }

    @Test
    public void shouldGetCurrentSizeofAllMemTables() throws Exception {
        runAndVerifySumOfProperties(CURRENT_SIZE_OF_ALL_MEMTABLES);
    }

    @Test
    public void shouldGetSizeofAllMemTables() throws Exception {
        runAndVerifySumOfProperties(SIZE_OF_ALL_MEMTABLES);
    }

    @Test
    public void shouldGetNumberOfEntriesActiveMemTable() throws Exception {
        runAndVerifySumOfProperties(NUMBER_OF_ENTRIES_ACTIVE_MEMTABLE);
    }

    @Test
    public void shouldGetNumberOfDeletesActiveMemTable() throws Exception {
        runAndVerifySumOfProperties(NUMBER_OF_DELETES_ACTIVE_MEMTABLE);
    }

    @Test
    public void shouldGetNumberOfEntriesImmutableMemTables() throws Exception {
        runAndVerifySumOfProperties(NUMBER_OF_ENTRIES_IMMUTABLE_MEMTABLES);
    }

    @Test
    public void shouldGetNumberOfDeletesImmutableMemTables() throws Exception {
        runAndVerifySumOfProperties(NUMBER_OF_DELETES_IMMUTABLE_MEMTABLES);
    }

    @Test
    public void shouldGetMemTableFlushPending() throws Exception {
        runAndVerifySumOfProperties(MEMTABLE_FLUSH_PENDING);
    }

    @Test
    public void shouldGetNumberOfRunningFlushes() throws Exception {
        runAndVerifySumOfProperties(NUMBER_OF_RUNNING_FLUSHES);
    }

    @Test
    public void shouldGetCompactionPending() throws Exception {
        runAndVerifySumOfProperties(COMPACTION_PENDING);
    }

    @Test
    public void shouldGetNumberOfRunningCompactions() throws Exception {
        runAndVerifySumOfProperties(NUMBER_OF_RUNNING_COMPACTIONS);
    }

    @Test
    public void shouldGetEstimatedBytesOfPendingCompactions() throws Exception {
        runAndVerifySumOfProperties(ESTIMATED_BYTES_OF_PENDING_COMPACTION);
    }

    @Test
    public void shouldGetTotalSstFilesSize() throws Exception {
        runAndVerifySumOfProperties(TOTAL_SST_FILES_SIZE);
    }

    @Test
    public void shouldGetLiveSstFilesSize() throws Exception {
        runAndVerifySumOfProperties(LIVE_SST_FILES_SIZE);
    }

    @Test
    public void shouldGetEstimatedNumberOfKeys() throws Exception {
        runAndVerifySumOfProperties(ESTIMATED_NUMBER_OF_KEYS);
    }

    @Test
    public void shouldGetEstimatedMemoryOfTableReaders() throws Exception {
        runAndVerifySumOfProperties(ESTIMATED_MEMORY_OF_TABLE_READERS);
    }

    @Test
    public void shouldGetNumberOfBackgroundErrors() throws Exception {
        runAndVerifySumOfProperties(NUMBER_OF_BACKGROUND_ERRORS);
    }

    @Test
    public void shouldGetCapacityOfBlockCacheWithMultipleCaches() throws Exception {
        runAndVerifyBlockCacheMetricsWithMultipleCaches(CAPACITY_OF_BLOCK_CACHE);
    }

    @Test
    public void shouldGetCapacityOfBlockCacheWithSingleCache() throws Exception {
        runAndVerifyBlockCacheMetricsWithSingleCache(CAPACITY_OF_BLOCK_CACHE);
    }

    @Test
    public void shouldGetUsageOfBlockCacheWithMultipleCaches() throws Exception {
        runAndVerifyBlockCacheMetricsWithMultipleCaches(USAGE_OF_BLOCK_CACHE);
    }

    @Test
    public void shouldGetUsageOfBlockCacheWithSingleCache() throws Exception {
        runAndVerifyBlockCacheMetricsWithSingleCache(USAGE_OF_BLOCK_CACHE);
    }

    @Test
    public void shouldGetPinnedUsageOfBlockCacheWithMultipleCaches() throws Exception {
        runAndVerifyBlockCacheMetricsWithMultipleCaches(PINNED_USAGE_OF_BLOCK_CACHE);
    }

    @Test
    public void shouldGetPinnedUsageOfBlockCacheWithSingleCache() throws Exception {
        runAndVerifyBlockCacheMetricsWithSingleCache(PINNED_USAGE_OF_BLOCK_CACHE);
    }

    private void runAndVerifySumOfProperties(final String propertyName) throws Exception {
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(new Metrics(), "test-client", StreamsConfig.METRICS_LATEST, new MockTime());
        final RocksDBMetricsRecorder recorder = new RocksDBMetricsRecorder(METRICS_SCOPE, STORE_NAME);

        recorder.init(streamsMetrics, TASK_ID);
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);
        recorder.addValueProviders(SEGMENT_STORE_NAME_2, dbToAdd2, cacheToAdd2, statisticsToAdd2);

        final long recordedValue1 = 5L;
        final long recordedValue2 = 3L;
        expect(dbToAdd1.getAggregatedLongProperty(ROCKSDB_PROPERTIES_PREFIX + propertyName))
            .andStubReturn(recordedValue1);
        expect(dbToAdd2.getAggregatedLongProperty(ROCKSDB_PROPERTIES_PREFIX + propertyName))
            .andStubReturn(recordedValue2);
        replay(dbToAdd1, dbToAdd2);

        verifyMetrics(streamsMetrics, propertyName, recordedValue1 + recordedValue2);
    }

    private void runAndVerifyBlockCacheMetricsWithMultipleCaches(final String propertyName) throws Exception {
        runAndVerifySumOfProperties(propertyName);
    }

    private void runAndVerifyBlockCacheMetricsWithSingleCache(final String propertyName) throws Exception {
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(new Metrics(), "test-client", StreamsConfig.METRICS_LATEST, new MockTime());
        final RocksDBMetricsRecorder recorder = new RocksDBMetricsRecorder(METRICS_SCOPE, STORE_NAME);

        recorder.init(streamsMetrics, TASK_ID);
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);
        recorder.addValueProviders(SEGMENT_STORE_NAME_2, dbToAdd2, cacheToAdd1, statisticsToAdd2);

        final long recordedValue = 5L;
        expect(dbToAdd1.getAggregatedLongProperty(ROCKSDB_PROPERTIES_PREFIX + propertyName))
            .andStubReturn(recordedValue);
        expect(dbToAdd2.getAggregatedLongProperty(ROCKSDB_PROPERTIES_PREFIX + propertyName))
            .andStubReturn(recordedValue);
        replay(dbToAdd1, dbToAdd2);

        verifyMetrics(streamsMetrics, propertyName, recordedValue);
    }

    private void verifyMetrics(final StreamsMetricsImpl streamsMetrics,
                               final String propertyName,
                               final long expectedValue) {

        final Map<MetricName, ? extends Metric> metrics = streamsMetrics.metrics();
        final Map<String, String> tagMap = mkMap(
            mkEntry(THREAD_ID_TAG, Thread.currentThread().getName()),
            mkEntry(TASK_ID_TAG, TASK_ID.toString()),
            mkEntry(METRICS_SCOPE + "-" + STORE_ID_TAG, STORE_NAME)
        );
        final KafkaMetric metric = (KafkaMetric) metrics.get(new MetricName(
            propertyName,
            STATE_STORE_LEVEL_GROUP,
            "description is ignored",
            tagMap
        ));

        assertThat(metric, notNullValue());
        assertThat(metric.metricValue(), is(BigInteger.valueOf(expectedValue)));
    }
}
