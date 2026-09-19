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

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

abstract class AbstractSegmentsTest<S extends Segments> {
    protected S segments;
    protected InternalMockProcessorContext<?, ?> context;
    private final Serializer<String> stringSerializer = new StringSerializer();
    private final Serializer<Long> longSerializer = new LongSerializer();


    abstract S getSegments();


    @BeforeEach
    public void setUp() {
        context = getProcessorContext();
        segments = getSegments();
        segments.openExisting(context, 0L);
    }

    private InternalMockProcessorContext<?, ?> getProcessorContext() {
        return new InternalMockProcessorContext<>(
                TestUtils.tempDirectory(),
                Serdes.String(),
                Serdes.Long(),
                new MockRecordCollector(),
                new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics())),
                new StreamsConfig(StreamsTestUtils.getStreamsConfig()));
    }

    private InternalMockProcessorContext<?, ?> getEOSProcessorContext() {
        final Properties streamsProps = StreamsTestUtils.getStreamsConfig();
        streamsProps.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        return new InternalMockProcessorContext<>(
                TestUtils.tempDirectory(),
                Serdes.String(),
                Serdes.Long(),
                new MockRecordCollector(),
                new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics())),
                new StreamsConfig(streamsProps));
    }

    @AfterEach
    public void tearDown() {
        segments.close();
    }

    @Test
    public void shouldThrowTaskCorruptedExceptionIfSegmentIsInInvalidState() throws Exception {
        context = getEOSProcessorContext();
        segments = getSegments();
        segments.openExisting(context, 0L);
        final Segment segment = segments.getOrCreateSegmentIfLive(0, context, 1L);

        assertTrue(segment.isOpen());
        segments.close();

        simulateUncleanShutdownForAllSegments();
        segments = getSegments();
        final TaskCorruptedException thrown = assertThrows(TaskCorruptedException.class, () -> segments.openExisting(context, 0L));
        assertEquals("Tasks [0_0] are corrupted and hence need to be re-initialized", thrown.getMessage());
    }

    @Test
    public void shouldNotRetainFailedSegmentSoRecoveryStaysRecoverable() throws Exception {
        context = getEOSProcessorContext();
        segments = getSegments();
        segments.openExisting(context, 0L);
        final Segment segment = segments.getOrCreateSegmentIfLive(0, context, 1L);

        assertTrue(segment.isOpen());
        segments.close();

        simulateUncleanShutdownForAllSegments();
        segments = getSegments();

        // The first open fails with a (recoverable) TaskCorruptedException because the on-disk state is dirty.
        assertThrows(TaskCorruptedException.class, () -> segments.openExisting(context, 0L));

        // A segment that failed to open must not be retained in the segments map. Otherwise a subsequent
        // open would return the stale, unopened segment instead of retrying to open it, and querying its
        // committed offset during task re-initialization would then fail with a fatal
        // InvalidStateStoreException. Re-opening must instead stay recoverable, i.e. keep throwing
        // TaskCorruptedException until the dirty local state is wiped.
        assertThrows(TaskCorruptedException.class, () -> segments.openExisting(context, 0L));
    }

    private void simulateUncleanShutdownForAllSegments() throws Exception {
        for (final File dbDir : Objects.requireNonNull(context.stateDir().listFiles())) {
            for (final File storeDir : Objects.requireNonNull(dbDir.listFiles())) {
                final DBOptions dbOptions = new DBOptions();
                final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
                final Long openState = 1L;
                final String dbPath = storeDir.getAbsolutePath();
                final List<ColumnFamilyDescriptor> existingColumnFamilies = RocksDB.listColumnFamilies(new Options(), dbPath).stream()
                        .map(b -> new ColumnFamilyDescriptor(b, columnFamilyOptions))
                        .collect(Collectors.toList());
                final List<ColumnFamilyHandle> columnFamilies = new ArrayList<>(existingColumnFamilies.size());
                RocksDB db = null;
                ColumnFamilyHandle offsetsColumnFamily = null;
                try {
                    db = RocksDB.open(
                            dbOptions,
                            storeDir.getAbsolutePath(),
                            existingColumnFamilies,
                            columnFamilies);
                    final byte[] statusKey = stringSerializer.serialize(null, "status");

                    offsetsColumnFamily = columnFamilies.get(columnFamilies.size() - 1);
                    db.put(offsetsColumnFamily, statusKey, longSerializer.serialize(null, openState));
                } finally {
                    if (db != null) {
                        db.close();
                    }
                    for (final ColumnFamilyHandle columnFamily : columnFamilies) {
                        columnFamily.close();
                    }
                    dbOptions.close();
                    columnFamilyOptions.close();
                }
            }
        }
    }

}