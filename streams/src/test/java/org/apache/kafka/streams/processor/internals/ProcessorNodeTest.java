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
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.MockProcessorContext;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class ProcessorNodeTest {

    @SuppressWarnings("unchecked")
    @Test (expected = StreamsException.class)
    public void shouldThrowStreamsExceptionIfExceptionCaughtDuringInit() throws Exception {
        final ProcessorNode node = new ProcessorNode("name", new ExceptionalProcessor(), Collections.emptySet());
        node.init(null);
    }

    @SuppressWarnings("unchecked")
    @Test (expected = StreamsException.class)
    public void shouldThrowStreamsExceptionIfExceptionCaughtDuringClose() throws Exception {
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

    @Test
    public void testMetrics() {
        final StateSerdes anyStateSerde = StateSerdes.withBuiltinTypes("anyName", Bytes.class, Bytes.class);

        final MockProcessorContext context = new MockProcessorContext(anyStateSerde,  new RecordCollectorImpl(null, null));
        final ProcessorNode node = new ProcessorNode("name", new NoOpProcessor(), Collections.emptySet());
        node.init(context);

        Metrics metrics = context.baseMetrics();
        String name = "task." + context.taskId();
        String[] latencyOperations = {"process", "punctuate", "create", "destroy"};
        String throughputOperation =  "forward";
        String groupName = "stream-processor-node-metrics";
        List<Map<String, String>> tags = new ArrayList<>();
        tags.add(Collections.singletonMap("processor-node-id", "all"));
        tags.add(Collections.singletonMap("processor-node-id", node.name()));


        for (String operation : latencyOperations) {
            assertNotNull(metrics.getSensor(operation));
        }
        assertNotNull(metrics.getSensor(throughputOperation));

        for (Map<String, String> tag : tags) {
            for (String operation : latencyOperations) {
                final String tmpName = tag.containsValue("all") ? operation :
                        name + "-" + operation;
                assertNotNull(metrics.metrics().get(metrics.metricName(tmpName + "-latency-avg", groupName,
                    "The average latency of " + tmpName + " operation.", tag)));
                assertNotNull(metrics.metrics().get(metrics.metricName(tmpName + "-latency-max", groupName,
                    "The max latency of " + tmpName + " operation.", tag)));
                assertNotNull(metrics.metrics().get(metrics.metricName(tmpName + "-rate", groupName,
                    "The average number of occurrence of " + operation + " operation per second.", tag)));
            }

            final String tmpName = tag.containsValue("all") ? throughputOperation :
                    name + "-" + throughputOperation;
            assertNotNull(metrics.metrics().get(metrics.metricName(tmpName + "-rate", groupName,
                "The average number of occurrence of " + tmpName + " operation per second.", tag)));
        }

        context.close();
    }

}