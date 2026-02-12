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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TimestampedSegmentsWithHeadersTest {

    private static final long SEGMENT_INTERVAL = 100L;
    private static final long RETENTION_PERIOD = 4 * SEGMENT_INTERVAL;
    private static final String METRICS_SCOPE = "test-state-id";
    private InternalMockProcessorContext context;
    private TimestampedSegmentsWithHeaders segments;
    private File stateDirectory;
    private final String storeName = "test";

    @BeforeEach
    public void createContext() {
        stateDirectory = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
            stateDirectory,
            Serdes.String(),
            Serdes.Long(),
            new MockRecordCollector(),
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics()))
        );
        segments = new TimestampedSegmentsWithHeaders(storeName, METRICS_SCOPE, RETENTION_PERIOD, SEGMENT_INTERVAL);
        segments.openExisting(context, -1L);
    }

    @AfterEach
    public void close() {
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
    public void shouldGetSegmentNameFromId() {
        assertEquals("test.0", segments.segmentName(0));
        assertEquals("test." + SEGMENT_INTERVAL, segments.segmentName(1));
        assertEquals("test." + 2 * SEGMENT_INTERVAL, segments.segmentName(2));
    }

    @Test
    public void shouldCreateSegments() {
        final TimestampedSegmentWithHeaders segment1 = segments.getOrCreateSegmentIfLive(0, context, -1L);
        final TimestampedSegmentWithHeaders segment2 = segments.getOrCreateSegmentIfLive(1, context, -1L);
        final TimestampedSegmentWithHeaders segment3 = segments.getOrCreateSegmentIfLive(2, context, -1L);

        assertTrue(new File(context.stateDir(), "test/test.0").isDirectory());
        assertTrue(new File(context.stateDir(), "test/test." + SEGMENT_INTERVAL).isDirectory());
        assertTrue(new File(context.stateDir(), "test/test." + 2 * SEGMENT_INTERVAL).isDirectory());
        assertTrue(segment1.isOpen());
        assertTrue(segment2.isOpen());
        assertTrue(segment3.isOpen());
    }

    @Test
    public void shouldNotCreateSegmentThatIsAlreadyExpired() {
        final long streamTime = updateStreamTimeAndCreateSegment(7);
        assertNull(segments.getOrCreateSegmentIfLive(0, context, streamTime));
        assertFalse(new File(context.stateDir(), "test/test.0").exists());
    }

    @Test
    public void shouldCleanupSegmentsThatHaveExpired() {
        final TimestampedSegmentWithHeaders segment1 = segments.getOrCreateSegmentIfLive(0, context, -1L);
        final TimestampedSegmentWithHeaders segment2 = segments.getOrCreateSegmentIfLive(1, context, -1L);
        final TimestampedSegmentWithHeaders segment3 = segments.getOrCreateSegmentIfLive(7, context, SEGMENT_INTERVAL * 7L);

        assertFalse(segment1.isOpen());
        assertFalse(segment2.isOpen());
        assertTrue(segment3.isOpen());
        assertFalse(new File(context.stateDir(), "test/test.0").exists());
        assertFalse(new File(context.stateDir(), "test/test." + SEGMENT_INTERVAL).exists());
        assertTrue(new File(context.stateDir(), "test/test." + 7 * SEGMENT_INTERVAL).exists());
    }

    @Test
    public void shouldGetSegmentForTimestamp() {
        final TimestampedSegmentWithHeaders segment = segments.getOrCreateSegmentIfLive(0, context, -1L);
        segments.getOrCreateSegmentIfLive(1, context, -1L);
        assertEquals(segment, segments.segmentForTimestamp(0L));
    }

    @Test
    public void shouldGetCorrectSegmentString() {
        final TimestampedSegmentWithHeaders segment = segments.getOrCreateSegmentIfLive(0, context, -1L);
        assertEquals("TimestampedSegmentWithHeaders(id=0, name=test.0)", segment.toString());
    }

    @Test
    public void shouldCloseAllOpenSegments() {
        final TimestampedSegmentWithHeaders first = segments.getOrCreateSegmentIfLive(0, context, -1L);
        final TimestampedSegmentWithHeaders second = segments.getOrCreateSegmentIfLive(1, context, -1L);
        final TimestampedSegmentWithHeaders third = segments.getOrCreateSegmentIfLive(2, context, -1L);
        segments.close();

        assertFalse(first.isOpen());
        assertFalse(second.isOpen());
        assertFalse(third.isOpen());
    }

    @Test
    public void shouldOpenExistingSegments() {
        segments = new TimestampedSegmentsWithHeaders("test", METRICS_SCOPE, 4, 1);
        segments.openExisting(context, -1L);
        segments.getOrCreateSegmentIfLive(0, context, -1L);
        segments.getOrCreateSegmentIfLive(1, context, -1L);
        segments.getOrCreateSegmentIfLive(2, context, -1L);
        segments.getOrCreateSegmentIfLive(3, context, -1L);
        segments.getOrCreateSegmentIfLive(4, context, -1L);
        // close existing.
        segments.close();

        segments = new TimestampedSegmentsWithHeaders("test", METRICS_SCOPE, 4, 1);
        segments.openExisting(context, -1L);

        assertTrue(segments.segmentForTimestamp(0).isOpen());
        assertTrue(segments.segmentForTimestamp(1).isOpen());
        assertTrue(segments.segmentForTimestamp(2).isOpen());
        assertTrue(segments.segmentForTimestamp(3).isOpen());
        assertTrue(segments.segmentForTimestamp(4).isOpen());
    }

    @Test
    public void shouldGetSegmentsWithinTimeRange() {
        updateStreamTimeAndCreateSegment(0);
        updateStreamTimeAndCreateSegment(1);
        updateStreamTimeAndCreateSegment(2);
        updateStreamTimeAndCreateSegment(3);
        final long streamTime = updateStreamTimeAndCreateSegment(4);
        segments.getOrCreateSegmentIfLive(0, context, streamTime);
        segments.getOrCreateSegmentIfLive(1, context, streamTime);
        segments.getOrCreateSegmentIfLive(2, context, streamTime);
        segments.getOrCreateSegmentIfLive(3, context, streamTime);
        segments.getOrCreateSegmentIfLive(4, context, streamTime);

        final List<TimestampedSegmentWithHeaders> segments = this.segments.segments(0, 2 * SEGMENT_INTERVAL, true);
        assertEquals(3, segments.size());
        assertEquals(0, segments.get(0).id);
        assertEquals(1, segments.get(1).id);
        assertEquals(2, segments.get(2).id);
    }

    @Test
    public void shouldClearSegmentsOnClose() {
        segments.getOrCreateSegmentIfLive(0, context, -1L);
        segments.close();
        assertNull(segments.segmentForTimestamp(0));
    }

    private long updateStreamTimeAndCreateSegment(final int segment) {
        final long streamTime = SEGMENT_INTERVAL * segment;
        segments.getOrCreateSegmentIfLive(segment, context, streamTime);
        return streamTime;
    }
}
