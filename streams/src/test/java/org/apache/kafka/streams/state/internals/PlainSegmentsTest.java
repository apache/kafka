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
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.SimpleTimeZone;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PlainSegmentsTest {

    private static final int NUM_SEGMENTS = 5;
    private static final long SEGMENT_INTERVAL = 100L;
    private static final long RETENTION_PERIOD = 4 * SEGMENT_INTERVAL;
    private InternalMockProcessorContext context;
    private PlainSegments segments;
    private File stateDirectory;
    private final String storeName = "test";

    @Before
    public void createContext() {
        stateDirectory = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext(
            stateDirectory,
            Serdes.String(),
            Serdes.Long(),
            new NoOpRecordCollector(),
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics()))
        );
        segments = new PlainSegments(storeName, RETENTION_PERIOD, SEGMENT_INTERVAL);
    }

    @After
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
    public void shouldBaseSegmentIntervalOnRetentionAndNumSegments() {
        final PlainSegments segments = new PlainSegments("test", 8 * SEGMENT_INTERVAL, 2 * SEGMENT_INTERVAL);
        assertEquals(0, segments.segmentId(0));
        assertEquals(0, segments.segmentId(SEGMENT_INTERVAL));
        assertEquals(1, segments.segmentId(2 * SEGMENT_INTERVAL));
    }

    @Test
    public void shouldGetSegmentNameFromId() {
        assertEquals("test.0", segments.segmentName(0));
        assertEquals("test." + SEGMENT_INTERVAL, segments.segmentName(1));
        assertEquals("test." + 2 * SEGMENT_INTERVAL, segments.segmentName(2));
    }

    @Test
    public void shouldCreateSegments() {
        final PlainSegment segment1 = segments.getOrCreateSegmentIfLive(0, context);
        final PlainSegment segment2 = segments.getOrCreateSegmentIfLive(1, context);
        final PlainSegment segment3 = segments.getOrCreateSegmentIfLive(2, context);
        assertTrue(new File(context.stateDir(), "test/test.0").isDirectory());
        assertTrue(new File(context.stateDir(), "test/test." + SEGMENT_INTERVAL).isDirectory());
        assertTrue(new File(context.stateDir(), "test/test." + 2 * SEGMENT_INTERVAL).isDirectory());
        assertTrue(segment1.isOpen());
        assertTrue(segment2.isOpen());
        assertTrue(segment3.isOpen());
    }

    @Test
    public void shouldNotCreateSegmentThatIsAlreadyExpired() {
        updateStreamTimeAndCreateSegment(7);
        assertNull(segments.getOrCreateSegmentIfLive(0, context));
        assertFalse(new File(context.stateDir(), "test/test.0").exists());
    }

    @Test
    public void shouldCleanupSegmentsThatHaveExpired() {
        final PlainSegment segment1 = segments.getOrCreateSegmentIfLive(0, context);
        final PlainSegment segment2 = segments.getOrCreateSegmentIfLive(1, context);
        context.setStreamTime(SEGMENT_INTERVAL * 7);
        final PlainSegment segment3 = segments.getOrCreateSegmentIfLive(7, context);
        assertFalse(segment1.isOpen());
        assertFalse(segment2.isOpen());
        assertTrue(segment3.isOpen());
        assertFalse(new File(context.stateDir(), "test/test.0").exists());
        assertFalse(new File(context.stateDir(), "test/test." + SEGMENT_INTERVAL).exists());
        assertTrue(new File(context.stateDir(), "test/test." + 7 * SEGMENT_INTERVAL).exists());
    }

    @Test
    public void shouldGetSegmentForTimestamp() {
        final PlainSegment segment = segments.getOrCreateSegmentIfLive(0, context);
        segments.getOrCreateSegmentIfLive(1, context);
        assertEquals(segment, segments.getSegmentForTimestamp(0L));
    }

    @Test
    public void shouldGetCorrectSegmentString() {
        final PlainSegment segment = segments.getOrCreateSegmentIfLive(0, context);
        assertEquals("PlainSegment(id=0, name=test.0)", segment.toString());
    }

    @Test
    public void shouldCloseAllOpenSegments() {
        final PlainSegment first = segments.getOrCreateSegmentIfLive(0, context);
        final PlainSegment second = segments.getOrCreateSegmentIfLive(1, context);
        final PlainSegment third = segments.getOrCreateSegmentIfLive(2, context);
        segments.close();

        assertFalse(first.isOpen());
        assertFalse(second.isOpen());
        assertFalse(third.isOpen());
    }

    @Test
    public void shouldOpenExistingSegments() {
        segments = new PlainSegments("test", 4, 1);
        segments.getOrCreateSegmentIfLive(0, context);
        segments.getOrCreateSegmentIfLive(1, context);
        segments.getOrCreateSegmentIfLive(2, context);
        segments.getOrCreateSegmentIfLive(3, context);
        segments.getOrCreateSegmentIfLive(4, context);
        // close existing.
        segments.close();

        segments = new PlainSegments("test", 4, 1);
        segments.openExisting(context);

        assertTrue(segments.getSegmentForTimestamp(0).isOpen());
        assertTrue(segments.getSegmentForTimestamp(1).isOpen());
        assertTrue(segments.getSegmentForTimestamp(2).isOpen());
        assertTrue(segments.getSegmentForTimestamp(3).isOpen());
        assertTrue(segments.getSegmentForTimestamp(4).isOpen());
    }

    @Test
    public void shouldGetSegmentsWithinTimeRange() {
        updateStreamTimeAndCreateSegment(0);
        updateStreamTimeAndCreateSegment(1);
        updateStreamTimeAndCreateSegment(2);
        updateStreamTimeAndCreateSegment(3);
        updateStreamTimeAndCreateSegment(4);
        segments.getOrCreateSegmentIfLive(0, context);
        segments.getOrCreateSegmentIfLive(1, context);
        segments.getOrCreateSegmentIfLive(2, context);
        segments.getOrCreateSegmentIfLive(3, context);
        segments.getOrCreateSegmentIfLive(4, context);

        final List<PlainSegment> segments = this.segments.segments(0, 2 * SEGMENT_INTERVAL);
        assertEquals(3, segments.size());
        assertEquals(0, segments.get(0).id);
        assertEquals(1, segments.get(1).id);
        assertEquals(2, segments.get(2).id);
    }

    @Test
    public void shouldGetSegmentsWithinTimeRangeOutOfOrder() {
        updateStreamTimeAndCreateSegment(4);
        updateStreamTimeAndCreateSegment(2);
        updateStreamTimeAndCreateSegment(0);
        updateStreamTimeAndCreateSegment(1);
        updateStreamTimeAndCreateSegment(3);

        final List<PlainSegment> segments = this.segments.segments(0, 2 * SEGMENT_INTERVAL);
        assertEquals(3, segments.size());
        assertEquals(0, segments.get(0).id);
        assertEquals(1, segments.get(1).id);
        assertEquals(2, segments.get(2).id);
    }

    @Test
    public void shouldRollSegments() {
        updateStreamTimeAndCreateSegment(0);
        verifyCorrectSegments(0, 1);
        updateStreamTimeAndCreateSegment(1);
        verifyCorrectSegments(0, 2);
        updateStreamTimeAndCreateSegment(2);
        verifyCorrectSegments(0, 3);
        updateStreamTimeAndCreateSegment(3);
        verifyCorrectSegments(0, 4);
        updateStreamTimeAndCreateSegment(4);
        verifyCorrectSegments(0, 5);
        updateStreamTimeAndCreateSegment(5);
        verifyCorrectSegments(1, 5);
        updateStreamTimeAndCreateSegment(6);
        verifyCorrectSegments(2, 5);
    }

    @Test
    public void futureEventsShouldNotCauseSegmentRoll() {
        updateStreamTimeAndCreateSegment(0);
        verifyCorrectSegments(0, 1);
        updateStreamTimeAndCreateSegment(1);
        verifyCorrectSegments(0, 2);
        updateStreamTimeAndCreateSegment(2);
        verifyCorrectSegments(0, 3);
        updateStreamTimeAndCreateSegment(3);
        verifyCorrectSegments(0, 4);
        updateStreamTimeAndCreateSegment(4);
        verifyCorrectSegments(0, 5);
        segments.getOrCreateSegmentIfLive(5, context);
        verifyCorrectSegments(0, 6);
        segments.getOrCreateSegmentIfLive(6, context);
        verifyCorrectSegments(0, 7);
    }

    private void updateStreamTimeAndCreateSegment(final int segment) {
        context.setStreamTime(SEGMENT_INTERVAL * segment);
        segments.getOrCreateSegmentIfLive(segment, context);
    }

    @Test
    public void shouldUpdateSegmentFileNameFromOldDateFormatToNewFormat() throws Exception {
        final long segmentInterval = 60_000L; // the old segment file's naming system maxes out at 1 minute granularity.

        segments = new PlainSegments(storeName, NUM_SEGMENTS * segmentInterval, segmentInterval);

        final String storeDirectoryPath = stateDirectory.getAbsolutePath() + File.separator + storeName;
        final File storeDirectory = new File(storeDirectoryPath);
        //noinspection ResultOfMethodCallIgnored
        storeDirectory.mkdirs();

        final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
        formatter.setTimeZone(new SimpleTimeZone(0, "UTC"));

        for (int segmentId = 0; segmentId < NUM_SEGMENTS; ++segmentId) {
            final File oldSegment = new File(storeDirectoryPath + File.separator + storeName + "-" + formatter.format(new Date(segmentId * segmentInterval)));
            //noinspection ResultOfMethodCallIgnored
            oldSegment.createNewFile();
        }

        segments.openExisting(context);

        for (int segmentId = 0; segmentId < NUM_SEGMENTS; ++segmentId) {
            final String segmentName = storeName + "." + (long) segmentId * segmentInterval;
            final File newSegment = new File(storeDirectoryPath + File.separator + segmentName);
            assertTrue(newSegment.exists());
        }
    }

    @Test
    public void shouldUpdateSegmentFileNameFromOldColonFormatToNewFormat() throws Exception {
        final String storeDirectoryPath = stateDirectory.getAbsolutePath() + File.separator + storeName;
        final File storeDirectory = new File(storeDirectoryPath);
        //noinspection ResultOfMethodCallIgnored
        storeDirectory.mkdirs();

        for (int segmentId = 0; segmentId < NUM_SEGMENTS; ++segmentId) {
            final File oldSegment = new File(storeDirectoryPath + File.separator + storeName + ":" + segmentId * (RETENTION_PERIOD / (NUM_SEGMENTS - 1)));
            //noinspection ResultOfMethodCallIgnored
            oldSegment.createNewFile();
        }

        segments.openExisting(context);

        for (int segmentId = 0; segmentId < NUM_SEGMENTS; ++segmentId) {
            final File newSegment = new File(storeDirectoryPath + File.separator + storeName + "." + segmentId * (RETENTION_PERIOD / (NUM_SEGMENTS - 1)));
            assertTrue(newSegment.exists());
        }
    }

    @Test
    public void shouldClearSegmentsOnClose() {
        segments.getOrCreateSegmentIfLive(0, context);
        segments.close();
        assertThat(segments.getSegmentForTimestamp(0), is(nullValue()));
    }

    private void verifyCorrectSegments(final long first, final int numSegments) {
        final List<PlainSegment> result = this.segments.segments(0, Long.MAX_VALUE);
        assertEquals(numSegments, result.size());
        for (int i = 0; i < numSegments; i++) {
            assertEquals(i + first, result.get(i).id);
        }
    }
}
