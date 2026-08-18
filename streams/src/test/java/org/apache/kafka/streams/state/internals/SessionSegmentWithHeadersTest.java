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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class SessionSegmentWithHeadersTest {

    private final RocksDBMetricsRecorder metricsRecorder =
        new RocksDBMetricsRecorder("metrics-scope", "store-name");

    @BeforeEach
    public void setUp() {
        metricsRecorder.init(
            new StreamsMetricsImpl(new Metrics(), "test-client", new MockTime()),
            new TaskId(0, 0)
        );
    }

    @Test
    public void shouldDeleteStateDirectoryOnDestroy() throws Exception {
        final SessionSegmentWithHeaders segment =
            new SessionSegmentWithHeaders("segment", "window", 0L, Position.emptyPosition(), metricsRecorder);
        final String directoryPath = TestUtils.tempDirectory().getAbsolutePath();
        final File directory = new File(directoryPath);

        segment.openDB(mkMap(mkEntry(METRICS_RECORDING_LEVEL_CONFIG, "INFO")), directory);

        assertTrue(new File(directoryPath, "window").exists());
        assertTrue(new File(directoryPath + File.separator + "window", "segment").exists());
        assertTrue(new File(directoryPath + File.separator + "window", "segment").list().length > 0);
        segment.destroy();
        assertFalse(new File(directoryPath + File.separator + "window", "segment").exists());
        assertTrue(new File(directoryPath, "window").exists());

        segment.close();
    }

    @Test
    public void shouldBeEqualIfIdIsEqual() {
        final SessionSegmentWithHeaders segment =
            new SessionSegmentWithHeaders("anyName", "anyName", 0L, Position.emptyPosition(), metricsRecorder);
        final SessionSegmentWithHeaders segmentSameId =
            new SessionSegmentWithHeaders("someOtherName", "someOtherName", 0L, Position.emptyPosition(), metricsRecorder);
        final SessionSegmentWithHeaders segmentDifferentId =
            new SessionSegmentWithHeaders("anyName", "anyName", 1L, Position.emptyPosition(), metricsRecorder);

        assertEquals(segment, segment);
        assertEquals(segment, segmentSameId);
        assertNotEquals(segment, segmentDifferentId);
        assertNotEquals(segment, null);
        assertNotEquals(segment, "anyName");

        segment.close();
        segmentSameId.close();
        segmentDifferentId.close();
    }

    @Test
    public void shouldHashOnSegmentIdOnly() {
        final SessionSegmentWithHeaders segment =
            new SessionSegmentWithHeaders("anyName", "anyName", 0L, Position.emptyPosition(), metricsRecorder);
        final SessionSegmentWithHeaders segmentSameId =
            new SessionSegmentWithHeaders("someOtherName", "someOtherName", 0L, Position.emptyPosition(), metricsRecorder);
        final SessionSegmentWithHeaders segmentDifferentId =
            new SessionSegmentWithHeaders("anyName", "anyName", 1L, Position.emptyPosition(), metricsRecorder);

        final Set<SessionSegmentWithHeaders> set = new HashSet<>();
        assertTrue(set.add(segment));
        assertFalse(set.add(segmentSameId));
        assertTrue(set.add(segmentDifferentId));

        segment.close();
        segmentSameId.close();
        segmentDifferentId.close();
    }

    @Test
    public void shouldCompareSegmentIdOnly() {
        final SessionSegmentWithHeaders segment1 =
            new SessionSegmentWithHeaders("a", "C", 50L, Position.emptyPosition(), metricsRecorder);
        final SessionSegmentWithHeaders segment2 =
            new SessionSegmentWithHeaders("b", "B", 100L, Position.emptyPosition(), metricsRecorder);
        final SessionSegmentWithHeaders segment3 =
            new SessionSegmentWithHeaders("c", "A", 0L, Position.emptyPosition(), metricsRecorder);

        assertEquals(0, segment1.compareTo(segment1));
        assertEquals(-1, segment1.compareTo(segment2));
        assertEquals(1, segment2.compareTo(segment1));
        assertEquals(1, segment1.compareTo(segment3));
        assertEquals(-1, segment3.compareTo(segment1));
        assertEquals(1, segment2.compareTo(segment3));
        assertEquals(-1, segment3.compareTo(segment2));

        segment1.close();
        segment2.close();
        segment3.close();
    }
}
