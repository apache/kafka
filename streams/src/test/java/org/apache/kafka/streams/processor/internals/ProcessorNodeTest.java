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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.kafka.streams.processor.internals.ProcessorNode.NodeMetrics.STREAM_PROCESSOR_NODE_METRICS;
import static org.apache.kafka.streams.processor.internals.StreamThread.StreamThreadMetrics.PROCESS;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESSOR_NODE_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RATE_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TASK_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TOTAL_SUFFIX;


public class ProcessorNodeTest {

    @SuppressWarnings("unchecked")
    @Test(expected = StreamsException.class)
    public void shouldThrowStreamsExceptionIfExceptionCaughtDuringInit() {
        final ProcessorNode node = new ProcessorNode("name", new ExceptionalProcessor(), Collections.emptySet());
        node.init(null);
    }

    @SuppressWarnings("unchecked")
    @Test(expected = StreamsException.class)
    public void shouldThrowStreamsExceptionIfExceptionCaughtDuringClose() {
        final ProcessorNode node = new ProcessorNode("name", new ExceptionalProcessor(), Collections.emptySet());
        node.close();
    }

    private static class ExceptionalProcessor implements Processor {
        @Override
        public void init(final ProcessorContext context) {
            throw new RuntimeException();
        }

        @Override
        public void process(final Object key, final Object value) {
            throw new RuntimeException();
        }

        @Override
        public void close() {
            throw new RuntimeException();
        }
    }

    @Test
    public void testMetrics() {
        final StateSerdes anyStateSerde = StateSerdes.withBuiltinTypes("anyName", Bytes.class, Bytes.class);

        final Metrics metrics = new Metrics();
        final InternalMockProcessorContext context = new InternalMockProcessorContext(
            anyStateSerde,
            new RecordCollectorImpl(
                null,
                new LogContext("processnode-test "),
                new DefaultProductionExceptionHandler(),
                metrics.sensor("skipped-records")
            ),
            metrics
        );

        final String[] latencyOperations = {PROCESS};
        final Map<String, String> metricTags = new LinkedHashMap<>();
        metricTags.put(PROCESSOR_NODE_ID_TAG, context.node.name);
        metricTags.put(TASK_ID_TAG, context.taskId().toString());

        for (final String opName : latencyOperations) {
            StreamsTestUtils.getMetricByNameFilterByTags(metrics.metrics(), opName + RATE_SUFFIX, STREAM_PROCESSOR_NODE_METRICS, metricTags);
            StreamsTestUtils.getMetricByNameFilterByTags(metrics.metrics(), opName + TOTAL_SUFFIX, STREAM_PROCESSOR_NODE_METRICS, metricTags);
        }
    }

}
