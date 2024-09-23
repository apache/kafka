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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogicalKeyValueSegmentsTest {

    private static final long SEGMENT_INTERVAL = 100L;
    private static final long RETENTION_PERIOD = 4 * SEGMENT_INTERVAL;
    private static final String STORE_NAME = "logical-segments";
    private static final String METRICS_SCOPE = "metrics-scope";
    private static final String DB_FILE_DIR = "rocksdb";

    private InternalMockProcessorContext context;

    private LogicalKeyValueSegments segments;

    @BeforeEach
    public void setUp() {
        context = new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            new MockRecordCollector(),
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics()))
        );
        segments = new LogicalKeyValueSegments(
            STORE_NAME,
            DB_FILE_DIR,
            RETENTION_PERIOD,
            SEGMENT_INTERVAL,
            new RocksDBMetricsRecorder(METRICS_SCOPE, STORE_NAME)
        );
        segments.setPosition(Position.emptyPosition());
        segments.openExisting(context, 0L);
    }

    @AfterEach
    public void tearDown() {
        segments.close();
    }

    @Test
    public void shouldGetSegmentIdsFromTimestamp() {
        assertEquals(0, segments.segmentId(0));
        assertEquals(1, segments.segmentId(SEGMENT_INTERVAL));
        assertEquals(2, segments.segmentId(2 * SEGMENT_INTERVAL));
        assertEquals(3, segments.segmentId(3 * SEGMENT_INTERVAL));
    }

    @Test
    public void shouldCreateSegments() {
        final LogicalKeyValueSegment segment1 = segments.getOrCreateSegmentIfLive(0, context, 0L);
        final LogicalKeyValueSegment segment2 = segments.getOrCreateSegmentIfLive(1, context, SEGMENT_INTERVAL);
        final LogicalKeyValueSegment segment3 = segments.getOrCreateSegmentIfLive(2, context, 2 * SEGMENT_INTERVAL);

        final File rocksdbDir = new File(new File(context.stateDir(), DB_FILE_DIR), STORE_NAME);
        assertTrue(rocksdbDir.isDirectory());

        assertTrue(segment1.isOpen());
        assertTrue(segment2.isOpen());
        assertTrue(segment3.isOpen());
    }

    @Test
    public void shouldNotCreateSegmentThatIsAlreadyExpired() {
        final long streamTime = updateStreamTimeAndCreateSegment(7);
        assertNull(segments.getOrCreateSegmentIfLive(0, context, streamTime));
    }

    @Test
    public void shouldCreateReservedSegments() {
        final LogicalKeyValueSegment reservedSegment1 = segments.createReservedSegment(-1, "reserved-1");
        final LogicalKeyValueSegment reservedSegment2 = segments.createReservedSegment(-2, "reserved-2");

        final File rocksdbDir = new File(new File(context.stateDir(), DB_FILE_DIR), STORE_NAME);
        assertTrue(rocksdbDir.isDirectory());

        assertTrue(reservedSegment1.isOpen());
        assertTrue(reservedSegment2.isOpen());
    }

    @Test
    public void shouldNotCreateReservedSegmentWithNonNegativeId() {
        assertThrows(IllegalArgumentException.class, () -> segments.createReservedSegment(0, "reserved"));
        assertThrows(IllegalArgumentException.class, () -> segments.createReservedSegment(1, "reserved"));
    }

    @Test
    public void shouldNotCreateReservedSegmentFromRegularMethod() {
        assertThrows(IllegalArgumentException.class, () -> segments.getOrCreateSegmentIfLive(-1, context, 0));
        assertThrows(IllegalArgumentException.class, () -> segments.getOrCreateSegment(-1, context));
    }

    @Test
    public void shouldCleanupSegmentsThatHaveExpired() {
        final LogicalKeyValueSegment segment1 = segments.getOrCreateSegmentIfLive(0, context, 0);
        final LogicalKeyValueSegment segment2 = segments.getOrCreateSegmentIfLive(2, context, SEGMENT_INTERVAL * 2L);
        final LogicalKeyValueSegment segment3 = segments.getOrCreateSegmentIfLive(3, context, SEGMENT_INTERVAL * 3L);
        final LogicalKeyValueSegment segment4 = segments.getOrCreateSegmentIfLive(7, context, SEGMENT_INTERVAL * 7L);

        segments.cleanupExpiredSegments(SEGMENT_INTERVAL * 7L);

        final List<LogicalKeyValueSegment> allSegments = segments.allSegments(true);
        assertEquals(2, allSegments.size());
        assertEquals(segment3, allSegments.get(0));
        assertEquals(segment4, allSegments.get(1));
    }

    @Test
    public void shouldNotCleanUpReservedSegments() {
        final LogicalKeyValueSegment reservedSegment = segments.createReservedSegment(-1, "reserved");
        final LogicalKeyValueSegment segment1 = segments.getOrCreateSegmentIfLive(1, context, SEGMENT_INTERVAL);
        final LogicalKeyValueSegment segment2 = segments.getOrCreateSegmentIfLive(2, context, SEGMENT_INTERVAL * 2L);

        segments.cleanupExpiredSegments(SEGMENT_INTERVAL * 6L);

        final List<LogicalKeyValueSegment> allSegments = segments.allSegments(true);
        assertEquals(1, allSegments.size());
        assertEquals(segment2, allSegments.get(0));
        assertEquals(reservedSegment, segments.getReservedSegment(-1));
    }

    @Test
    public void shouldGetSegmentForTimestamp() {
        final LogicalKeyValueSegment segment1 = segments.getOrCreateSegmentIfLive(0, context, 0L);
        final LogicalKeyValueSegment segment2 = segments.getOrCreateSegmentIfLive(1, context, SEGMENT_INTERVAL);

        assertEquals(segment1, segments.segmentForTimestamp(0L));
        assertEquals(segment1, segments.segmentForTimestamp(SEGMENT_INTERVAL - 1));
        assertEquals(segment2, segments.segmentForTimestamp(SEGMENT_INTERVAL));
        assertEquals(segment2, segments.segmentForTimestamp(2 * SEGMENT_INTERVAL - 1));
    }

    @Test
    public void shouldGetSegmentsWithinTimeRange() {
        // presence of reserved segment changes nothing (reserved segments are not returned from segments())
        segments.createReservedSegment(-1, "reserved");

        final long streamTime = updateStreamTimeAndCreateSegment(4);
        segments.getOrCreateSegmentIfLive(0, context, streamTime);
        segments.getOrCreateSegmentIfLive(2, context, streamTime);
        segments.getOrCreateSegmentIfLive(1, context, streamTime); // intentionally out of order for test
        segments.getOrCreateSegmentIfLive(3, context, streamTime);
        segments.getOrCreateSegmentIfLive(4, context, streamTime);

        final List<LogicalKeyValueSegment> segments = this.segments.segments(0, 2 * SEGMENT_INTERVAL, true);
        assertEquals(3, segments.size());
        assertEquals(0, segments.get(0).id());
        assertEquals(1, segments.get(1).id());
        assertEquals(2, segments.get(2).id());
    }

    @Test
    public void shouldGetSegmentsWithinBackwardTimeRange() {
        // presence of reserved segment changes nothing (reserved segments are not returned from segments())
        segments.createReservedSegment(-1, "reserved");

        final long streamTime = updateStreamTimeAndCreateSegment(4);
        segments.getOrCreateSegmentIfLive(0, context, streamTime);
        segments.getOrCreateSegmentIfLive(2, context, streamTime);
        segments.getOrCreateSegmentIfLive(1, context, streamTime); // intentionally out of order for test
        segments.getOrCreateSegmentIfLive(3, context, streamTime);
        segments.getOrCreateSegmentIfLive(4, context, streamTime);

        final List<LogicalKeyValueSegment> segments = this.segments.segments(0, 2 * SEGMENT_INTERVAL, false);
        assertEquals(3, segments.size());
        assertEquals(2, segments.get(0).id());
        assertEquals(1, segments.get(1).id());
        assertEquals(0, segments.get(2).id());
    }

    @Test
    public void shouldClearSegmentsOnClose() {
        final LogicalKeyValueSegment segment = segments.getOrCreateSegmentIfLive(0, context, 0L);
        final LogicalKeyValueSegment reservedSegment = segments.createReservedSegment(-1, "reserved");

        // add data and open some iterators to verify that they are properly closed later
        segment.put(new Bytes("k".getBytes()), "v".getBytes());
        reservedSegment.put(new Bytes("k".getBytes()), "v".getBytes());
        final KeyValueIterator<Bytes, byte[]> all1 = segment.all();
        final KeyValueIterator<Bytes, byte[]> all2 = reservedSegment.all();
        assertTrue(all1.hasNext());
        assertTrue(all2.hasNext());

        segments.close();

        assertThat(segments.segmentForTimestamp(0), is(nullValue()));
        assertThat(segments.getReservedSegment(-1), is(nullValue()));
        // verify iterators closed as well
        assertThrows(InvalidStateStoreException.class, all1::hasNext);
        assertThrows(InvalidStateStoreException.class, all2::hasNext);
    }

    private long updateStreamTimeAndCreateSegment(final int segment) {
        final long streamTime = SEGMENT_INTERVAL * segment;
        segments.getOrCreateSegmentIfLive(segment, context, streamTime);
        return streamTime;
    }
}