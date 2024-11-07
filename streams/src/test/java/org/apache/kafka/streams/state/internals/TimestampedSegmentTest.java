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
import org.apache.kafka.streams.processor.ProcessorContext;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class TimestampedSegmentTest {

    private final RocksDBMetricsRecorder metricsRecorder =
        new RocksDBMetricsRecorder("metrics-scope", "store-name");

    @BeforeEach
    public void setUp() {
        metricsRecorder.init(
            new StreamsMetricsImpl(new Metrics(), "test-client", "processId", new MockTime()),
            new TaskId(0, 0)
        );
    }

    @Test
    public void shouldDeleteStateDirectoryOnDestroy() throws Exception {
        final TimestampedSegment segment = new TimestampedSegment("segment", "window", 0L, Position.emptyPosition(), metricsRecorder);
        final String directoryPath = TestUtils.tempDirectory().getAbsolutePath();
        final File directory = new File(directoryPath);

        final ProcessorContext mockContext = mock(ProcessorContext.class);
        when(mockContext.appConfigs()).thenReturn(mkMap(mkEntry(METRICS_RECORDING_LEVEL_CONFIG, "INFO")));
        when(mockContext.stateDir()).thenReturn(directory);

        segment.openDB(mockContext.appConfigs(), mockContext.stateDir());

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
        final TimestampedSegment segment = new TimestampedSegment("anyName", "anyName", 0L, Position.emptyPosition(), metricsRecorder);
        final TimestampedSegment segmentSameId =
            new TimestampedSegment("someOtherName", "someOtherName", 0L, Position.emptyPosition(), metricsRecorder);
        final TimestampedSegment segmentDifferentId =
            new TimestampedSegment("anyName", "anyName", 1L, Position.emptyPosition(), metricsRecorder);

        assertThat(segment, equalTo(segment));
        assertThat(segment, equalTo(segmentSameId));
        assertThat(segment, not(equalTo(segmentDifferentId)));
        assertThat(segment, not(equalTo(null)));
        assertThat(segment, not(equalTo("anyName")));

        segment.close();
        segmentSameId.close();
        segmentDifferentId.close();
    }

    @Test
    public void shouldHashOnSegmentIdOnly() {
        final TimestampedSegment segment = new TimestampedSegment("anyName", "anyName", 0L, Position.emptyPosition(), metricsRecorder);
        final TimestampedSegment segmentSameId =
            new TimestampedSegment("someOtherName", "someOtherName", 0L, Position.emptyPosition(), metricsRecorder);
        final TimestampedSegment segmentDifferentId =
            new TimestampedSegment("anyName", "anyName", 1L, Position.emptyPosition(), metricsRecorder);

        final Set<TimestampedSegment> set = new HashSet<>();
        assertTrue(set.add(segment));
        assertFalse(set.add(segmentSameId));
        assertTrue(set.add(segmentDifferentId));

        segment.close();
        segmentSameId.close();
        segmentDifferentId.close();
    }

    @Test
    public void shouldCompareSegmentIdOnly() {
        final TimestampedSegment segment1 = new TimestampedSegment("a", "C", 50L, Position.emptyPosition(), metricsRecorder);
        final TimestampedSegment segment2 = new TimestampedSegment("b", "B", 100L, Position.emptyPosition(), metricsRecorder);
        final TimestampedSegment segment3 = new TimestampedSegment("c", "A", 0L, Position.emptyPosition(), metricsRecorder);

        assertThat(segment1.compareTo(segment1), equalTo(0));
        assertThat(segment1.compareTo(segment2), equalTo(-1));
        assertThat(segment2.compareTo(segment1), equalTo(1));
        assertThat(segment1.compareTo(segment3), equalTo(1));
        assertThat(segment3.compareTo(segment1), equalTo(-1));
        assertThat(segment2.compareTo(segment3), equalTo(1));
        assertThat(segment3.compareTo(segment2), equalTo(-1));

        segment1.close();
        segment2.close();
        segment3.close();
    }
}
