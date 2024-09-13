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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LogSegmentsTest {
    private final TopicPartition topicPartition = new TopicPartition("topic", 0);
    private static File logDir = null;

    /* create a segment with the given base offset */
    private static LogSegment createSegment(Long offset) throws IOException {
        return LogTestUtils.createSegment(offset, logDir, 10, Time.SYSTEM);
    }

    @BeforeEach
    public void setup() {
        logDir = TestUtils.tempDirectory();
    }

    @AfterEach
    public void teardown() throws IOException {
        Utils.delete(logDir);
    }

    private void assertEntry(LogSegment segment, Map.Entry<Long, LogSegment> tested) {
        assertEquals(segment.baseOffset(), tested.getKey());
        assertEquals(segment, tested.getValue());
    }

    @Test
    public void testBasicOperations() throws IOException {
        long offset1 = 40;
        long offset2 = 80;
        try (LogSegments segments = new LogSegments(topicPartition);
             LogSegment seg1 = createSegment(offset1);
             LogSegment seg2 = createSegment(offset2);
             LogSegment seg3 = createSegment(offset1)) {

            assertTrue(segments.isEmpty());
            assertFalse(segments.nonEmpty());

            // Add seg1
            segments.add(seg1);
            assertFalse(segments.isEmpty());
            assertTrue(segments.nonEmpty());
            assertEquals(1, segments.numberOfSegments());
            assertTrue(segments.contains(offset1));
            assertEquals(Optional.of(seg1), segments.get(offset1));

            // Add seg2
            segments.add(seg2);
            assertFalse(segments.isEmpty());
            assertTrue(segments.nonEmpty());
            assertEquals(2, segments.numberOfSegments());
            assertTrue(segments.contains(offset2));
            assertEquals(Optional.of(seg2), segments.get(offset2));

            // Replace seg1 with seg3
            segments.add(seg3);
            assertFalse(segments.isEmpty());
            assertTrue(segments.nonEmpty());
            assertEquals(2, segments.numberOfSegments());
            assertTrue(segments.contains(offset1));
            assertEquals(Optional.of(seg3), segments.get(offset1));

            // Remove seg2
            segments.remove(offset2);
            assertFalse(segments.isEmpty());
            assertTrue(segments.nonEmpty());
            assertEquals(1, segments.numberOfSegments());
            assertFalse(segments.contains(offset2));

            // Clear all segments including seg3
            segments.clear();
            assertTrue(segments.isEmpty());
            assertFalse(segments.nonEmpty());
            assertEquals(0, segments.numberOfSegments());
            assertFalse(segments.contains(offset1));
        }
    }

    @Test
    public void testSegmentAccess() throws IOException {
        try (LogSegments segments = new LogSegments(topicPartition)) {
            long offset1 = 1;
            LogSegment seg1 = createSegment(offset1);
            long offset2 = 2;
            LogSegment seg2 = createSegment(offset2);
            long offset3 = 3;
            LogSegment seg3 = createSegment(offset3);
            long offset4 = 4;
            LogSegment seg4 = createSegment(offset4);

            // Test firstEntry, lastEntry
            List<LogSegment> segmentList = Arrays.asList(seg1, seg2, seg3, seg4);
            for (LogSegment seg : segmentList) {
                segments.add(seg);
                assertEntry(seg1, segments.firstEntry().get());
                assertEquals(Optional.of(seg1), segments.firstSegment());
                assertEquals(OptionalLong.of(1), segments.firstSegmentBaseOffset());
                assertEntry(seg, segments.lastEntry().get());
                assertEquals(Optional.of(seg), segments.lastSegment());
            }

            // Test baseOffsets
            assertEquals(Arrays.asList(offset1, offset2, offset3, offset4), segments.baseOffsets());

            // Test values
            assertEquals(Arrays.asList(seg1, seg2, seg3, seg4), new ArrayList<>(segments.values()));

            // Test values(to, from)
            assertThrows(IllegalArgumentException.class, () -> segments.values(2, 1));
            assertEquals(Collections.emptyList(), segments.values(1, 1));
            assertEquals(Collections.singletonList(seg1), new ArrayList<>(segments.values(1, 2)));
            assertEquals(Arrays.asList(seg1, seg2), new ArrayList<>(segments.values(1, 3)));
            assertEquals(Arrays.asList(seg1, seg2, seg3), new ArrayList<>(segments.values(1, 4)));
            assertEquals(Arrays.asList(seg2, seg3), new ArrayList<>(segments.values(2, 4)));
            assertEquals(Collections.singletonList(seg3), new ArrayList<>(segments.values(3, 4)));
            assertEquals(Collections.emptyList(), new ArrayList<>(segments.values(4, 4)));
            assertEquals(Collections.singletonList(seg4), new ArrayList<>(segments.values(4, 5)));

            // Test activeSegment
            assertEquals(seg4, segments.activeSegment());

            // Test nonActiveLogSegmentsFrom
            assertEquals(Arrays.asList(seg2, seg3), new ArrayList<>(segments.nonActiveLogSegmentsFrom(2)));
            assertEquals(Collections.emptyList(), new ArrayList<>(segments.nonActiveLogSegmentsFrom(4)));
        }
    }

    @Test
    public void testClosestMatchOperations() throws IOException {
        try (LogSegments segments = new LogSegments(topicPartition)) {
            LogSegment seg1 = createSegment(1L);
            LogSegment seg2 = createSegment(3L);
            LogSegment seg3 = createSegment(5L);
            LogSegment seg4 = createSegment(7L);

            Arrays.asList(seg1, seg2, seg3, seg4).forEach(segments::add);

            // Test floorSegment
            assertEquals(Optional.of(seg1), segments.floorSegment(2));
            assertEquals(Optional.of(seg2), segments.floorSegment(3));

            // Test lowerSegment
            assertEquals(Optional.of(seg1), segments.lowerSegment(3));
            assertEquals(Optional.of(seg2), segments.lowerSegment(4));

            // Test higherSegment, higherEntry
            assertEquals(Optional.of(seg3), segments.higherSegment(4));
            assertEntry(seg3, segments.higherEntry(4).get());
            assertEquals(Optional.of(seg4), segments.higherSegment(5));
            assertEntry(seg4, segments.higherEntry(5).get());
        }
    }

    @Test
    public void testHigherSegments() throws IOException {
        try (LogSegments segments = new LogSegments(topicPartition)) {
            LogSegment seg1 = createSegment(1L);
            LogSegment seg2 = createSegment(3L);
            LogSegment seg3 = createSegment(5L);
            LogSegment seg4 = createSegment(7L);
            LogSegment seg5 = createSegment(9L);

            Arrays.asList(seg1, seg2, seg3, seg4, seg5).forEach(segments::add);

            // higherSegments(0) should return all segments in order
            {
                final Iterator<LogSegment> iterator = segments.higherSegments(0).iterator();
                Arrays.asList(seg1, seg2, seg3, seg4, seg5).forEach(segment -> {
                    assertTrue(iterator.hasNext());
                    assertEquals(segment, iterator.next());
                });
                assertFalse(iterator.hasNext());
            }

            // higherSegments(1) should return all segments in order except seg1
            {
                final Iterator<LogSegment> iterator = segments.higherSegments(1).iterator();
                Arrays.asList(seg2, seg3, seg4, seg5).forEach(segment -> {
                    assertTrue(iterator.hasNext());
                    assertEquals(segment, iterator.next());
                });
                assertFalse(iterator.hasNext());
            }

            // higherSegments(8) should return only seg5
            {
                final Iterator<LogSegment> iterator = segments.higherSegments(8).iterator();
                assertTrue(iterator.hasNext());
                assertEquals(seg5, iterator.next());
                assertFalse(iterator.hasNext());
            }

            // higherSegments(9) should return no segments
            {
                final Iterator<LogSegment> iterator = segments.higherSegments(9).iterator();
                assertFalse(iterator.hasNext());
            }
        }
    }

    @Test
    public void testSizeForLargeLogs() throws IOException {
        try (LogSegment logSegment = mock(LogSegment.class)) {
            long largeSize = (long) Integer.MAX_VALUE * 2;

            when(logSegment.size()).thenReturn(Integer.MAX_VALUE);

            assertEquals(Integer.MAX_VALUE, LogSegments.sizeInBytes(Collections.singletonList(logSegment)));
            assertEquals(largeSize, LogSegments.sizeInBytes(Arrays.asList(logSegment, logSegment)));
            assertTrue(LogSegments.sizeInBytes(Arrays.asList(logSegment, logSegment)) > Integer.MAX_VALUE);

            try (LogSegments logSegments = new LogSegments(topicPartition)) {
                logSegments.add(logSegment);
                assertEquals(Integer.MAX_VALUE, logSegments.sizeInBytes());
            }
        }
    }

    @Test
    public void testUpdateDir() throws IOException {
        try (LogSegment seg1 = createSegment(1L);
             LogSegments segments = new LogSegments(topicPartition)) {

            segments.add(seg1);
            File newDir = TestUtils.tempDirectory();
            segments.updateParentDir(newDir);
            assertEquals(newDir, seg1.log().file().getParentFile());
            assertEquals(newDir, seg1.timeIndexFile().getParentFile());
            assertEquals(newDir, seg1.offsetIndexFile().getParentFile());
            assertEquals(newDir, seg1.txnIndex().file().getParentFile());

            Utils.delete(newDir);
        }
    }

}
