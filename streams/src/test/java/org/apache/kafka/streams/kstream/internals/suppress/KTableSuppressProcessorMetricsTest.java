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
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.FullChangeSerde;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.InMemoryTimeOrderedKeyValueBuffer;
import org.apache.kafka.test.MockInternalProcessorContext;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.time.Duration;
import java.util.Map;

import static org.apache.kafka.common.serialization.Serdes.Long;
import static org.apache.kafka.common.serialization.Serdes.String;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxRecords;
import static org.apache.kafka.streams.processor.internals.ProcessorNode.NodeMetrics.STREAM_PROCESSOR_NODE_METRICS;
import static org.apache.kafka.streams.processor.internals.ProcessorNode.NodeMetrics.SUPPRESSION_EMIT_RECORDS;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.AVG_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.BUFFER_STRING;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.CURRENT_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.ID_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.MAX_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESSOR_NODE_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RATE_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TASK_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.THREAD_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TOTAL_SUFFIX;
import static org.apache.kafka.streams.state.internals.StoreMetrics.SUPPRESSION_BUFFER_COUNT;
import static org.apache.kafka.streams.state.internals.StoreMetrics.SUPPRESSION_BUFFER_SIZE;
import static org.apache.kafka.test.StreamsTestUtils.getMetricByName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;

@SuppressWarnings("PointlessArithmeticExpression")
public class KTableSuppressProcessorMetricsTest {
    private static final long ARBITRARY_LONG = 5L;
    private static final String TEST_NODE = "test-node";
    private static final String TEST_STORE = "test-store";

    private static final MetricName EVICTION_TOTAL_METRIC = new MetricName(
        SUPPRESSION_EMIT_RECORDS + TOTAL_SUFFIX,
        STREAM_PROCESSOR_NODE_METRICS,
        "The total number of occurrence of suppression-emit operations.",
        mkMap(
            mkEntry(THREAD_ID_TAG, "mock-processor-context-virtual-thread"),
            mkEntry(TASK_ID_TAG, "0_0"),
            mkEntry(PROCESSOR_NODE_ID_TAG, TEST_NODE)
        )
    );

    private static final MetricName EVICTION_RATE_METRIC = new MetricName(
        SUPPRESSION_EMIT_RECORDS + RATE_SUFFIX,
        STREAM_PROCESSOR_NODE_METRICS,
        "The average number of occurrence of suppression-emit operation per second.",
        mkMap(
            mkEntry(THREAD_ID_TAG, "mock-processor-context-virtual-thread"),
            mkEntry(TASK_ID_TAG, "0_0"),
            mkEntry(PROCESSOR_NODE_ID_TAG, TEST_NODE)
        )
    );

    private static final MetricName BUFFER_SIZE_AVG_METRIC = new MetricName(
        SUPPRESSION_BUFFER_SIZE + AVG_SUFFIX,
        StreamsMetricsImpl.groupNameFromScope(BUFFER_STRING),
        "The average size of buffered records.",
        mkMap(
            mkEntry(THREAD_ID_TAG, "mock-processor-context-virtual-thread"),
            mkEntry(TASK_ID_TAG, "0_0"),
            mkEntry(BUFFER_STRING + ID_SUFFIX, TEST_STORE)
        )
    );

    private static final MetricName BUFFER_SIZE_CURRENT_METRIC = new MetricName(
        SUPPRESSION_BUFFER_SIZE + CURRENT_SUFFIX,
        StreamsMetricsImpl.groupNameFromScope(BUFFER_STRING),
        "The current size of buffered records.",
        mkMap(
            mkEntry(THREAD_ID_TAG, "mock-processor-context-virtual-thread"),
            mkEntry(TASK_ID_TAG, "0_0"),
            mkEntry(BUFFER_STRING + ID_SUFFIX, TEST_STORE)
        )
    );

    private static final MetricName BUFFER_SIZE_MAX_METRIC = new MetricName(
        SUPPRESSION_BUFFER_SIZE + MAX_SUFFIX,
        StreamsMetricsImpl.groupNameFromScope(BUFFER_STRING),
        "The max size of buffered records.",
        mkMap(
            mkEntry(THREAD_ID_TAG, "mock-processor-context-virtual-thread"),
            mkEntry(TASK_ID_TAG, "0_0"),
            mkEntry(BUFFER_STRING + ID_SUFFIX, TEST_STORE)
        )
    );

    private static final MetricName BUFFER_COUNT_AVG_METRIC = new MetricName(
        SUPPRESSION_BUFFER_COUNT + AVG_SUFFIX,
        StreamsMetricsImpl.groupNameFromScope(BUFFER_STRING),
        "The average count of buffered records.",
        mkMap(
            mkEntry(THREAD_ID_TAG, "mock-processor-context-virtual-thread"),
            mkEntry(TASK_ID_TAG, "0_0"),
            mkEntry(BUFFER_STRING + ID_SUFFIX, TEST_STORE)
        )
    );

    private static final MetricName BUFFER_COUNT_CURRENT_METRIC = new MetricName(
        SUPPRESSION_BUFFER_COUNT + CURRENT_SUFFIX,
        StreamsMetricsImpl.groupNameFromScope(BUFFER_STRING),
        "The current count of buffered records.",
        mkMap(
            mkEntry(THREAD_ID_TAG, "mock-processor-context-virtual-thread"),
            mkEntry(TASK_ID_TAG, "0_0"),
            mkEntry(BUFFER_STRING + ID_SUFFIX, TEST_STORE)
        )
    );

    private static final MetricName BUFFER_COUNT_MAX_METRIC = new MetricName(
        SUPPRESSION_BUFFER_COUNT + MAX_SUFFIX,
        StreamsMetricsImpl.groupNameFromScope(BUFFER_STRING),
        "The max count of buffered records.",
        mkMap(
            mkEntry(THREAD_ID_TAG, "mock-processor-context-virtual-thread"),
            mkEntry(TASK_ID_TAG, "0_0"),
            mkEntry(BUFFER_STRING + ID_SUFFIX, TEST_STORE)
        )
    );

    @Test
    public void shouldRecordMetrics() {
        final String storeName = TEST_STORE;

        final StateStore buffer = new InMemoryTimeOrderedKeyValueBuffer.Builder(storeName)
            .withLoggingDisabled()
            .build();

        final KTableSuppressProcessor<String, Long> processor =
            new KTableSuppressProcessor<>(
                (SuppressedInternal<String>) Suppressed.<String>untilTimeLimit(Duration.ofDays(100), maxRecords(1)),
                storeName,
                String(),
                new FullChangeSerde<>(Long())
            );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        final ProcessorNode processorNode = new ProcessorNode(TEST_NODE);
        context.setCurrentNode(processorNode);
        processorNode.init(context);

        buffer.init(context, buffer);
        processor.init(context);

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        final String key = "longKey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

        {
            final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();

            verifyMetric(metrics, EVICTION_RATE_METRIC, is(0.0));
            verifyMetric(metrics, EVICTION_TOTAL_METRIC, is(0.0));
            verifyMetric(metrics, BUFFER_SIZE_AVG_METRIC, is(25.5));
            verifyMetric(metrics, BUFFER_SIZE_CURRENT_METRIC, is(51.0));
            verifyMetric(metrics, BUFFER_SIZE_MAX_METRIC, is(51.0));
            verifyMetric(metrics, BUFFER_COUNT_AVG_METRIC, is(0.5));
            verifyMetric(metrics, BUFFER_COUNT_CURRENT_METRIC, is(1.0));
            verifyMetric(metrics, BUFFER_COUNT_MAX_METRIC, is(1.0));
        }

        context.setRecordMetadata("", 0, 1L, null, timestamp + 1);
        processor.process("key", value);

        {
            final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();

            verifyMetric(metrics, EVICTION_RATE_METRIC, greaterThan(0.0));
            verifyMetric(metrics, EVICTION_TOTAL_METRIC, is(1.0));
            verifyMetric(metrics, BUFFER_SIZE_AVG_METRIC, is(49.0));
            verifyMetric(metrics, BUFFER_SIZE_CURRENT_METRIC, is(47.0));
            verifyMetric(metrics, BUFFER_SIZE_MAX_METRIC, is(98.0));
            verifyMetric(metrics, BUFFER_COUNT_AVG_METRIC, is(1.0));
            verifyMetric(metrics, BUFFER_COUNT_CURRENT_METRIC, is(1.0));
            verifyMetric(metrics, BUFFER_COUNT_MAX_METRIC, is(2.0));
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void verifyMetric(final Map<MetricName, ? extends Metric> metrics,
                                  final MetricName metricName,
                                  final Matcher<T> matcher) {
        assertThat((T) getMetricByName(metrics, metricName.name(), metricName.group()).metricValue(), matcher);
    }
}