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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
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

public class TimestampedSegmentsTest {

    private static final int NUM_SEGMENTS = 5;
    private static final long SEGMENT_INTERVAL = 100L;
    private static final long RETENTION_PERIOD = 4 * SEGMENT_INTERVAL;
    private static final String METRICS_SCOPE = "test-state-id";
    private InternalMockProcessorContext context;
    private TimestampedSegments segments;
    private File stateDirectory;
    private final String storeName = "test";

    @Before
    public void createContext() {
        stateDirectory = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
            stateDirectory,
            Serdes.String(),
            Serdes.Long(),
            new MockRecordCollector(),
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics()))
        );
        segments = new TimestampedSegments(storeName, METRICS_SCOPE, RETENTION_PERIOD, SEGMENT_INTERVAL);
        segments.openExisting(context, -1L);
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
        final TimestampedSegments segments =
            new TimestampedSegments("test", METRICS_SCOPE, 8 * SEGMENT_INTERVAL, 2 * SEGMENT_INTERVAL);
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
        final TimestampedSegment segment1 = segments.getOrCreateSegmentIfLive(0, context, -1L);
        final TimestampedSegment segment2 = segments.getOrCreateSegmentIfLive(1, context, -1L);
        final TimestampedSegment segment3 = segments.getOrCreateSegmentIfLive(2, context, -1L);
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
        final TimestampedSegment segment1 = segments.getOrCreateSegmentIfLive(0, context, -1L);
        final TimestampedSegment segment2 = segments.getOrCreateSegmentIfLive(1, context, -1L);
        final TimestampedSegment segment3 = segments.getOrCreateSegmentIfLive(7, context, SEGMENT_INTERVAL * 7L);
        assertFalse(segment1.isOpen());
        assertFalse(segment2.isOpen());
        assertTrue(segment3.isOpen());
        assertFalse(new File(context.stateDir(), "test/test.0").exists());
        assertFalse(new File(context.stateDir(), "test/test." + SEGMENT_INTERVAL).exists());
        assertTrue(new File(context.stateDir(), "test/test." + 7 * SEGMENT_INTERVAL).exists());
    }

    @Test
    public void shouldGetSegmentForTimestamp() {
        final TimestampedSegment segment = segments.getOrCreateSegmentIfLive(0, context, -1L);
        segments.getOrCreateSegmentIfLive(1, context, -1L);
        assertEquals(segment, segments.getSegmentForTimestamp(0L));
    }

    @Test
    public void shouldGetCorrectSegmentString() {
        final TimestampedSegment segment = segments.getOrCreateSegmentIfLive(0, context, -1L);
        assertEquals("TimestampedSegment(id=0, name=test.0)", segment.toString());
    }

    @Test
    public void shouldCloseAllOpenSegments() {
        final TimestampedSegment first = segments.getOrCreateSegmentIfLive(0, context, -1L);
        final TimestampedSegment second = segments.getOrCreateSegmentIfLive(1, context, -1L);
        final TimestampedSegment third = segments.getOrCreateSegmentIfLive(2, context, -1L);
        segments.close();

        assertFalse(first.isOpen());
        assertFalse(second.isOpen());
        assertFalse(third.isOpen());
    }

    @Test
    public void shouldOpenExistingSegments() {
        segments = new TimestampedSegments("test", METRICS_SCOPE, 4, 1);
        segments.openExisting(context, -1L);
        segments.getOrCreateSegmentIfLive(0, context, -1L);
        segments.getOrCreateSegmentIfLive(1, context, -1L);
        segments.getOrCreateSegmentIfLive(2, context, -1L);
        segments.getOrCreateSegmentIfLive(3, context, -1L);
        segments.getOrCreateSegmentIfLive(4, context, -1L);
        // close existing.
        segments.close();

        segments = new TimestampedSegments("test", METRICS_SCOPE, 4, 1);
        segments.openExisting(context, -1L);

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
        final long streamTime = updateStreamTimeAndCreateSegment(4);
        segments.getOrCreateSegmentIfLive(0, context, streamTime);
        segments.getOrCreateSegmentIfLive(1, context, streamTime);
        segments.getOrCreateSegmentIfLive(2, context, streamTime);
        segments.getOrCreateSegmentIfLive(3, context, streamTime);
        segments.getOrCreateSegmentIfLive(4, context, streamTime);

        final List<TimestampedSegment> segments = this.segments.segments(0, 2 * SEGMENT_INTERVAL, true);
        assertEquals(3, segments.size());
        assertEquals(0, segments.get(0).id);
        assertEquals(1, segments.get(1).id);
        assertEquals(2, segments.get(2).id);
    }

    @Test
    public void shouldGetSegmentsWithinBackwardTimeRange() {
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

        final List<TimestampedSegment> segments = this.segments.segments(0, 2 * SEGMENT_INTERVAL, false);
        assertEquals(3, segments.size());
        assertEquals(0, segments.get(2).id);
        assertEquals(1, segments.get(1).id);
        assertEquals(2, segments.get(0).id);
    }

    @Test
    public void shouldGetSegmentsWithinTimeRangeOutOfOrder() {
        updateStreamTimeAndCreateSegment(4);
        updateStreamTimeAndCreateSegment(2);
        updateStreamTimeAndCreateSegment(0);
        updateStreamTimeAndCreateSegment(1);
        updateStreamTimeAndCreateSegment(3);

        final List<TimestampedSegment> segments = this.segments.segments(0, 2 * SEGMENT_INTERVAL, true);
        assertEquals(3, segments.size());
        assertEquals(0, segments.get(0).id);
        assertEquals(1, segments.get(1).id);
        assertEquals(2, segments.get(2).id);
    }

    @Test
    public void shouldGetSegmentsWithinBackwardTimeRangeOutOfOrder() {
        updateStreamTimeAndCreateSegment(4);
        updateStreamTimeAndCreateSegment(2);
        updateStreamTimeAndCreateSegment(0);
        updateStreamTimeAndCreateSegment(1);
        updateStreamTimeAndCreateSegment(3);

        final List<TimestampedSegment> segments = this.segments.segments(0, 2 * SEGMENT_INTERVAL, false);
        assertEquals(3, segments.size());
        assertEquals(0, segments.get(2).id);
        assertEquals(1, segments.get(1).id);
        assertEquals(2, segments.get(0).id);
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
        final long streamTime = updateStreamTimeAndCreateSegment(4);
        verifyCorrectSegments(0, 5);
        segments.getOrCreateSegmentIfLive(5, context, streamTime);
        verifyCorrectSegments(0, 6);
        segments.getOrCreateSegmentIfLive(6, context, streamTime);
        verifyCorrectSegments(0, 7);
    }

    private long updateStreamTimeAndCreateSegment(final int segment) {
        final long streamTime = SEGMENT_INTERVAL * segment;
        segments.getOrCreateSegmentIfLive(segment, context, streamTime);
        return streamTime;
    }

    @Test
    public void shouldUpdateSegmentFileNameFromOldDateFormatToNewFormat() throws Exception {
        final long segmentInterval = 60_000L; // the old segment file's naming system maxes out at 1 minute granularity.

        segments = new TimestampedSegments(storeName, METRICS_SCOPE, NUM_SEGMENTS * segmentInterval, segmentInterval);

        final String storeDirectoryPath = stateDirectory.getAbsolutePath() + File.separator + storeName;
        final File storeDirectory = new File(storeDirectoryPath);
        //noinspection ResultOfMethodCallIgnored
        storeDirectory.mkdirs();

        final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
        formatter.setTimeZone(new SimpleTimeZone(0, "UTC"));

        for (int segmentId = 0; segmentId < NUM_SEGMENTS; ++segmentId) {
            final File oldSegment = new File(storeDirectoryPath + File.separator + storeName + "-" + formatter.format(new Date(segmentId * segmentInterval)));
            //noinspection ResultOfMethodCallIgnored
            Files.createFile(oldSegment.toPath());
        }

        segments.openExisting(context, -1L);

        for (int segmentId = 0; segmentId < NUM_SEGMENTS; ++segmentId) {
            final String segmentName = storeName + "." + (long) segmentId * segmentInterval;
            final File newSegment = new File(storeDirectoryPath + File.separator + segmentName);
            assertTrue(Files.exists(newSegment.toPath()));
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
            Files.createFile(oldSegment.toPath());
        }

        segments.openExisting(context, -1L);

        for (int segmentId = 0; segmentId < NUM_SEGMENTS; ++segmentId) {
            final File newSegment = new File(storeDirectoryPath + File.separator + storeName + "." + segmentId * (RETENTION_PERIOD / (NUM_SEGMENTS - 1)));
            assertTrue(Files.exists(newSegment.toPath()));
        }
    }

    @Test
    public void shouldClearSegmentsOnClose() {
        segments.getOrCreateSegmentIfLive(0, context, -1L);
        segments.close();
        assertThat(segments.getSegmentForTimestamp(0), is(nullValue()));
    }

    private void verifyCorrectSegments(final long first, final int numSegments) {
        final List<TimestampedSegment> result = this.segments.segments(0, Long.MAX_VALUE, true);
        assertEquals(numSegments, result.size());
        for (int i = 0; i < numSegments; i++) {
            assertEquals(i + first, result.get(i).id);
        }
    }
}
