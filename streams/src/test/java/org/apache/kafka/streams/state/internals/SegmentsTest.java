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
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SegmentsTest {

    private static final int NUM_SEGMENTS = 5;
    private MockProcessorContext context;
    private Segments segments;
    private long segmentInterval;

    @Before
    public void createContext() {
        context = new MockProcessorContext(TestUtils.tempDirectory(),
                                           Serdes.String(),
                                           Serdes.Long(),
                                           new NoOpRecordCollector(),
                                           new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics())));
        int retentionPeriod = 4 * 60 * 1000;
        segments = new Segments("test", retentionPeriod, NUM_SEGMENTS);
        segmentInterval = Segments.segmentInterval(retentionPeriod, NUM_SEGMENTS);
    }

    @After
    public void close() {
        context.close();
        segments.close();
    }

    @Test
    public void shouldGetSegmentIdsFromTimestamp() {
        assertEquals(0, segments.segmentId(0));
        assertEquals(1, segments.segmentId(60000));
        assertEquals(2, segments.segmentId(120000));
        assertEquals(3, segments.segmentId(180000));
    }

    @Test
    public void shouldBaseSegmentIntervalOnRetentionAndNumSegments() {
        final Segments segments = new Segments("test", 8 * 60 * 1000, 5);
        assertEquals(0, segments.segmentId(0));
        assertEquals(0, segments.segmentId(60000));
        assertEquals(1, segments.segmentId(120000));
    }

    @Test
    public void shouldGetSegmentNameFromId() throws Exception {
        assertEquals("test:0", segments.segmentName(0));
        assertEquals("test:" + segmentInterval, segments.segmentName(1));
        assertEquals("test:" + 2 * segmentInterval, segments.segmentName(2));
    }

    @Test
    public void shouldCreateSegments() {
        final Segment segment1 = segments.getOrCreateSegment(0, context);
        final Segment segment2 = segments.getOrCreateSegment(1, context);
        final Segment segment3 = segments.getOrCreateSegment(2, context);
        assertTrue(new File(context.stateDir(), "test/test:0").isDirectory());
        assertTrue(new File(context.stateDir(), "test/test:" + segmentInterval).isDirectory());
        assertTrue(new File(context.stateDir(), "test/test:" + 2 * segmentInterval).isDirectory());
        assertEquals(true, segment1.isOpen());
        assertEquals(true, segment2.isOpen());
        assertEquals(true, segment3.isOpen());
    }

    @Test
    public void shouldNotCreateSegmentThatIsAlreadyExpired() {
        segments.getOrCreateSegment(7, context);
        assertNull(segments.getOrCreateSegment(0, context));
        assertFalse(new File(context.stateDir(), "test/test:0").exists());
    }

    @Test
    public void shouldCleanupSegmentsThatHaveExpired() {
        final Segment segment1 = segments.getOrCreateSegment(0, context);
        final Segment segment2 = segments.getOrCreateSegment(1, context);
        final Segment segment3 = segments.getOrCreateSegment(7, context);
        assertFalse(segment1.isOpen());
        assertFalse(segment2.isOpen());
        assertTrue(segment3.isOpen());
        assertFalse(new File(context.stateDir(), "test/test:0").exists());
        assertFalse(new File(context.stateDir(), "test/test:" + segmentInterval).exists());
        assertTrue(new File(context.stateDir(), "test/test:" + 7 * segmentInterval).exists());
    }

    @Test
    public void shouldGetSegmentForTimestamp() {
        final Segment segment = segments.getOrCreateSegment(0, context);
        segments.getOrCreateSegment(1, context);
        assertEquals(segment, segments.getSegmentForTimestamp(0L));
    }

    @Test
    public void shouldCloseAllOpenSegments() {
        final Segment first = segments.getOrCreateSegment(0, context);
        final Segment second = segments.getOrCreateSegment(1, context);
        final Segment third = segments.getOrCreateSegment(2, context);
        segments.close();

        assertFalse(first.isOpen());
        assertFalse(second.isOpen());
        assertFalse(third.isOpen());
    }

    @Test
    public void shouldOpenExistingSegments() {
        segments.getOrCreateSegment(0, context);
        segments.getOrCreateSegment(1, context);
        segments.getOrCreateSegment(2, context);
        segments.getOrCreateSegment(3, context);
        segments.getOrCreateSegment(4, context);
        // close existing.
        segments.close();

        segments = new Segments("test", 4 * 60 * 1000, 5);
        segments.openExisting(context);

        assertTrue(segments.getSegmentForTimestamp(0).isOpen());
        assertTrue(segments.getSegmentForTimestamp(1).isOpen());
        assertTrue(segments.getSegmentForTimestamp(2).isOpen());
        assertTrue(segments.getSegmentForTimestamp(3).isOpen());
        assertTrue(segments.getSegmentForTimestamp(4).isOpen());
    }

    @Test
    public void shouldGetSegmentsWithinTimeRange() {
        segments.getOrCreateSegment(0, context);
        segments.getOrCreateSegment(1, context);
        segments.getOrCreateSegment(2, context);
        segments.getOrCreateSegment(3, context);
        segments.getOrCreateSegment(4, context);

        final List<Segment> segments = this.segments.segments(0, 2 * 60 * 1000);
        assertEquals(3, segments.size());
        assertEquals(0, segments.get(0).id);
        assertEquals(1, segments.get(1).id);
        assertEquals(2, segments.get(2).id);
    }

    @Test
    public void shouldGetSegmentsWithinTimeRangeOutOfOrder() throws Exception {
        segments.getOrCreateSegment(4, context);
        segments.getOrCreateSegment(2, context);
        segments.getOrCreateSegment(0, context);
        segments.getOrCreateSegment(1, context);
        segments.getOrCreateSegment(3, context);

        final List<Segment> segments = this.segments.segments(0, 2 * 60 * 1000);
        assertEquals(3, segments.size());
        assertEquals(0, segments.get(0).id);
        assertEquals(1, segments.get(1).id);
        assertEquals(2, segments.get(2).id);
    }

    @Test
    public void shouldRollSegments() {
        segments.getOrCreateSegment(0, context);
        verifyCorrectSegments(0, 1);
        segments.getOrCreateSegment(1, context);
        verifyCorrectSegments(0, 2);
        segments.getOrCreateSegment(2, context);
        verifyCorrectSegments(0, 3);
        segments.getOrCreateSegment(3, context);
        verifyCorrectSegments(0, 4);
        segments.getOrCreateSegment(4, context);
        verifyCorrectSegments(0, 5);
        segments.getOrCreateSegment(5, context);
        verifyCorrectSegments(1, 5);
        segments.getOrCreateSegment(6, context);
        verifyCorrectSegments(2, 5);
    }

    private void verifyCorrectSegments(final long first, final int numSegments) {
        final List<Segment> result = this.segments.segments(0, Long.MAX_VALUE);
        assertEquals(numSegments, result.size());
        for (int i = 0; i < numSegments; i++) {
            assertEquals(i + first, result.get(i).id);
        }
    }
}
