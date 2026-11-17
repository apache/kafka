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
package org.apache.kafka.streams.kstream.internals.suppress;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableImpl;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.InMemoryTimeOrderedKeyValueChangeBuffer;
import org.apache.kafka.test.MockInternalProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxRecords;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class KTableSuppressProcessorMetricsTest {
    private static final long ARBITRARY_LONG = 5L;
    private static final TaskId TASK_ID = new TaskId(0, 0);
    private final Properties streamsConfig = StreamsTestUtils.getStreamsConfig();
    private final String threadId = Thread.currentThread().getName();

    private final MetricName evictionTotalMetricLatest = new MetricName(
        "suppression-emit-total",
        "stream-processor-node-metrics",
        "The total number of emitted records from the suppression buffer",
        mkMap(
            mkEntry("thread-id", threadId),
            mkEntry("task-id", TASK_ID.toString()),
            mkEntry("processor-node-id", "testNode")
        )
    );

    private final MetricName evictionRateMetricLatest = new MetricName(
        "suppression-emit-rate",
        "stream-processor-node-metrics",
        "The average number of emitted records from the suppression buffer per second",
        mkMap(
            mkEntry("thread-id", threadId),
            mkEntry("task-id", TASK_ID.toString()),
            mkEntry("processor-node-id", "testNode")
        )
    );

    private final MetricName bufferSizeAvgMetricLatest = new MetricName(
        "suppression-buffer-size-avg",
        "stream-state-metrics",
        "The average size of buffered records",
        mkMap(
            mkEntry("thread-id", threadId),
            mkEntry("task-id", TASK_ID.toString()),
            mkEntry("in-memory-suppression-state-id", "test-store")
        )
    );

    private final MetricName bufferSizeMaxMetricLatest = new MetricName(
        "suppression-buffer-size-max",
        "stream-state-metrics",
        "The maximum size of buffered records",
        mkMap(
            mkEntry("thread-id", threadId),
            mkEntry("task-id", TASK_ID.toString()),
            mkEntry("in-memory-suppression-state-id", "test-store")
        )
    );

    private final MetricName bufferCountAvgMetricLatest = new MetricName(
        "suppression-buffer-count-avg",
        "stream-state-metrics",
        "The average count of buffered records",
        mkMap(
            mkEntry("thread-id", threadId),
            mkEntry("task-id", TASK_ID.toString()),
            mkEntry("in-memory-suppression-state-id", "test-store")
        )
    );

    private final MetricName bufferCountMaxMetricLatest = new MetricName(
        "suppression-buffer-count-max",
        "stream-state-metrics",
        "The maximum count of buffered records",
        mkMap(
            mkEntry("thread-id", threadId),
            mkEntry("task-id", TASK_ID.toString()),
            mkEntry("in-memory-suppression-state-id", "test-store")
        )
    );

    @Test
    public void shouldRecordMetricsWithBuiltInMetricsVersionLatest() {
        final String storeName = "test-store";

        final StateStore buffer = new InMemoryTimeOrderedKeyValueChangeBuffer.Builder<>(
            storeName, Serdes.String(),
            Serdes.Long()
        )
            .withLoggingDisabled()
            .build();

        @SuppressWarnings("unchecked")
        final KTableImpl<String, ?, Long> mock = mock(KTableImpl.class);
        final Processor<String, Change<Long>, String, Change<Long>> processor =
            new KTableSuppressProcessorSupplier<>(
                (SuppressedInternal<String>) Suppressed.<String>untilTimeLimit(Duration.ofDays(100), maxRecords(1)),
                mockBuilderWithName(storeName),
                mock
            ).get();

        streamsConfig.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, StreamsConfig.METRICS_LATEST);
        final MockInternalProcessorContext<String, Change<Long>> context =
            new MockInternalProcessorContext<>(streamsConfig, TASK_ID, TestUtils.tempDirectory());
        final Time time = Time.SYSTEM;
        context.setCurrentNode(new ProcessorNode<>("testNode"));
        context.setSystemTimeMs(time.milliseconds());

        buffer.init(context, buffer);
        processor.init(context);

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L);
        context.setTimestamp(timestamp);
        final String key = "longKey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(new Record<>(key, value, timestamp));

        {
            final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();

            verifyMetric(metrics, evictionRateMetricLatest, 0.0);
            verifyMetric(metrics, evictionTotalMetricLatest, 0.0);
            verifyMetric(metrics, bufferSizeAvgMetricLatest, 21.5);
            verifyMetric(metrics, bufferSizeMaxMetricLatest, 43.0);
            verifyMetric(metrics, bufferCountAvgMetricLatest, 0.5);
            verifyMetric(metrics, bufferCountMaxMetricLatest, 1.0);
        }

        context.setRecordMetadata("", 0, 1L);
        context.setTimestamp(timestamp + 1);
        processor.process(new Record<>("key", value, timestamp + 1));

        {
            final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();

            assertTrue((double) metrics.get(evictionRateMetricLatest).metricValue() > 0.0);
            verifyMetric(metrics, evictionTotalMetricLatest, 1.0);
            verifyMetric(metrics, bufferSizeAvgMetricLatest, 41.0);
            verifyMetric(metrics, bufferSizeMaxMetricLatest, 82.0);
            verifyMetric(metrics, bufferCountAvgMetricLatest, 1.0);
            verifyMetric(metrics, bufferCountMaxMetricLatest, 2.0);
        }
    }

    private static void verifyMetric(final Map<MetricName, ? extends Metric> metrics,
                                     final MetricName metricName,
                                     final double expected) {
        assertEquals(metricName.description(), metrics.get(metricName).metricName().description());
        assertEquals(expected, metrics.get(metricName).metricValue());
    }

    private StoreBuilder<?> mockBuilderWithName(final String name) {
        final StoreBuilder<?> builder = Mockito.mock(StoreBuilder.class);
        Mockito.when(builder.name()).thenReturn(name);
        return builder;
    }
}