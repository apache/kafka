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

import static org.junit.Assert.assertNotNull;

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

    private static class NoOpProcessor implements Processor<Object, Object> {
        @Override
        public void init(final ProcessorContext context) {

        }

        @Override
        public void process(final Object key, final Object value) {

        }

        @Override
        public void close() {

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
                null,
                new LogContext("processnode-test "),
                new DefaultProductionExceptionHandler(),
                metrics.sensor("skipped-records")
            ),
            metrics
        );
        final ProcessorNode<Object, Object> node = new ProcessorNode<>("name", new NoOpProcessor(), Collections.<String>emptySet());
        node.init(context);

        final String[] latencyOperations = {"process", "punctuate", "create", "destroy"};
        final String throughputOperation = "forward";
        final String groupName = "stream-processor-node-metrics";
        final Map<String, String> metricTags = new LinkedHashMap<>();
        metricTags.put("processor-node-id", node.name());
        metricTags.put("task-id", context.taskId().toString());
        metricTags.put("client-id", "mock");


        for (final String operation : latencyOperations) {
            assertNotNull(metrics.getSensor("name-mock.0_0." + operation));
        }
        assertNotNull(metrics.getSensor("name-mock.0_0." + throughputOperation));

        for (final String opName : latencyOperations) {
            StreamsTestUtils.getMetricByNameFilterByTags(metrics.metrics(), "mock.0_0." + opName + "-latency-avg", groupName, metricTags);
            StreamsTestUtils.getMetricByNameFilterByTags(metrics.metrics(), "mock.0_0." + opName + "-latency-max", groupName, metricTags);
            StreamsTestUtils.getMetricByNameFilterByTags(metrics.metrics(), "mock.0_0." + opName + "-rate", groupName, metricTags);
            StreamsTestUtils.getMetricByNameFilterByTags(metrics.metrics(), "mock.0_0." + opName + "-total", groupName, metricTags);
        }
        assertNotNull(metrics.metrics().get(metrics.metricName("mock.0_0." + throughputOperation + "-rate", groupName,
            "The average number of occurrence of " + "mock.0_0." + throughputOperation + " operation per second.", metricTags)));

        // test "all"
        metricTags.put("processor-node-id", "all");
        for (final String opName : latencyOperations) {
            StreamsTestUtils.getMetricByNameFilterByTags(metrics.metrics(), "mock.0_0." + opName + "-latency-avg", groupName, metricTags);
            StreamsTestUtils.getMetricByNameFilterByTags(metrics.metrics(), "mock.0_0." + opName + "-latency-max", groupName, metricTags);
            StreamsTestUtils.getMetricByNameFilterByTags(metrics.metrics(), "mock.0_0." + opName + "-rate", groupName, metricTags);
            StreamsTestUtils.getMetricByNameFilterByTags(metrics.metrics(), "mock.0_0." + opName + "-total", groupName, metricTags);
        }
        assertNotNull(metrics.metrics().get(metrics.metricName("mock.0_0." + throughputOperation + "-rate", groupName,
            "The average number of occurrence of " + "mock.0_0." + throughputOperation + " operation per second.", metricTags)));


    }

}
