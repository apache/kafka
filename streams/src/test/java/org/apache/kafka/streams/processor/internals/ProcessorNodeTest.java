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
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.MockProcessorContext;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.LinkedHashMap;

import static org.junit.Assert.assertNotNull;

public class ProcessorNodeTest {

    @SuppressWarnings("unchecked")
    @Test (expected = StreamsException.class)
    public void shouldThrowStreamsExceptionIfExceptionCaughtDuringInit() {
        final ProcessorNode node = new ProcessorNode("name", new ExceptionalProcessor(), Collections.emptySet());
        node.init(null);
    }

    @SuppressWarnings("unchecked")
    @Test (expected = StreamsException.class)
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
        public void punctuate(final long timestamp) {
            throw new RuntimeException();
        }

        @Override
        public void close() {
            throw new RuntimeException();
        }
    }

    private static class NoOpProcessor implements Processor {
        @Override
        public void init(final ProcessorContext context) {

        }

        @Override
        public void process(final Object key, final Object value) {

        }

        @Override
        public void punctuate(final long timestamp) {

        }

        @Override
        public void close() {

        }
    }

    private void testSpecificMetrics(final Metrics metrics, final String groupName,
                                     final String opName,
                                     final Map<String, String> metricTags) {
        assertNotNull(metrics.metrics().get(metrics.metricName(opName + "-latency-avg", groupName,
                "The average latency of " + opName + " operation.", metricTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(opName + "-latency-max", groupName,
                "The max latency of " + opName + " operation.", metricTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(opName + "-rate", groupName,
                "The average number of occurrence of " + opName + " operation per second.", metricTags)));

    }

    @Test
    public void testMetrics() {
        final StateSerdes anyStateSerde = StateSerdes.withBuiltinTypes("anyName", Bytes.class, Bytes.class);

        final Metrics metrics = new Metrics();
        final MockProcessorContext context = new MockProcessorContext(anyStateSerde,  new RecordCollectorImpl(null, null, new LogContext("processnode-test ")), metrics);
        final ProcessorNode node = new ProcessorNode("name", new NoOpProcessor(), Collections.emptySet());
        node.init(context);

        String[] latencyOperations = {"process", "punctuate", "create", "destroy"};
        String throughputOperation =  "forward";
        String groupName = "stream-processor-node-metrics";
        final Map<String, String> metricTags = new LinkedHashMap<>();
        metricTags.put("processor-node-id", node.name());
        metricTags.put("task-id", context.taskId().toString());


        for (String operation : latencyOperations) {
            assertNotNull(metrics.getSensor(operation));
        }
        assertNotNull(metrics.getSensor(throughputOperation));

        for (String opName : latencyOperations) {
            testSpecificMetrics(metrics, groupName, opName, metricTags);
        }
        assertNotNull(metrics.metrics().get(metrics.metricName(throughputOperation + "-rate", groupName,
                "The average number of occurrence of " + throughputOperation + " operation per second.", metricTags)));

        // test "all"
        metricTags.put("processor-node-id", "all");
        for (String opName : latencyOperations) {
            testSpecificMetrics(metrics, groupName, opName, metricTags);
        }
        assertNotNull(metrics.metrics().get(metrics.metricName(throughputOperation + "-rate", groupName,
                "The average number of occurrence of " + throughputOperation + " operation per second.", metricTags)));


        context.close();
    }

}