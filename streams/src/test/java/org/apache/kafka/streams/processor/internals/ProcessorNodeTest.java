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

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.test.MockInternalProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.ROLLUP_VALUE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ProcessorNodeTest {

    public static final String METRIC_GROUP = "stream-processor-node-metrics";

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

    private static class ExceptionalProcessor implements Processor<Object, Object> {
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
    public void testMetricsWithBuiltInMetricsVersionLatest() {
        testMetrics(StreamsConfig.METRICS_LATEST);
    }

    @Test
    public void testMetricsWithBuiltInMetricsVersion0100To24() {
        testMetrics(StreamsConfig.METRICS_0100_TO_24);
    }

    private void testMetrics(final String builtInMetricsVersion) {
        final Properties properties = StreamsTestUtils.getStreamsConfig();
        properties.put(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, builtInMetricsVersion);
        final InternalProcessorContext<Object, Object> context = new MockInternalProcessorContext(properties, new Metrics());
        final Map<MetricName, ? extends Metric> metrics = context.metrics().metrics();
        final ProcessorNode<Object, Object> node = new ProcessorNode<>("name", new NoOpProcessor(), Collections.emptySet());
        node.init(context);

        final String threadId = Thread.currentThread().getName();
        final String[] latencyOperations = {"process", "punctuate", "create", "destroy"};
        final Map<String, String> metricTags = new LinkedHashMap<>();
        final String threadIdTagKey =
            StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion) ? "client-id" : "thread-id";
        metricTags.put("processor-node-id", node.name());
        metricTags.put("task-id", context.taskId().toString());
        metricTags.put(threadIdTagKey, threadId);

        if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
            for (final String opName : latencyOperations) {
                assertTrue(StreamsTestUtils.containsMetric(metrics, opName + "-latency-avg", METRIC_GROUP, metricTags));
                assertTrue(StreamsTestUtils.containsMetric(metrics, opName + "-latency-max", METRIC_GROUP, metricTags));
                assertTrue(StreamsTestUtils.containsMetric(metrics, opName + "-rate", METRIC_GROUP, metricTags));
                assertTrue(StreamsTestUtils.containsMetric(metrics, opName + "-total", METRIC_GROUP, metricTags));
            }

            // test parent sensors
            metricTags.put("processor-node-id", ROLLUP_VALUE);
            for (final String opName : latencyOperations) {
                assertTrue(StreamsTestUtils.containsMetric(metrics, opName + "-latency-avg", METRIC_GROUP, metricTags));
                assertTrue(StreamsTestUtils.containsMetric(metrics, opName + "-latency-max", METRIC_GROUP, metricTags));
                assertTrue(StreamsTestUtils.containsMetric(metrics, opName + "-rate", METRIC_GROUP, metricTags));
                assertTrue(StreamsTestUtils.containsMetric(metrics, opName + "-total", METRIC_GROUP, metricTags));
            }
        } else {
            for (final String opName : latencyOperations) {
                assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-latency-avg", METRIC_GROUP, metricTags));
                assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-latency-max", METRIC_GROUP, metricTags));
                assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-rate", METRIC_GROUP, metricTags));
                assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-total", METRIC_GROUP, metricTags));
            }

            // test parent sensors
            metricTags.put("processor-node-id", ROLLUP_VALUE);
            for (final String opName : latencyOperations) {
                assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-latency-avg", METRIC_GROUP, metricTags));
                assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-latency-max", METRIC_GROUP, metricTags));
                assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-rate", METRIC_GROUP, metricTags));
                assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-total", METRIC_GROUP, metricTags));
            }
        }
    }

    @Test
    public void testTopologyLevelClassCastException() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        // Serdes configuration is missing (default will be used which don't match the DSL below), which will trigger the new exception
        final StreamsBuilder builder = new StreamsBuilder();

        builder.<String, String>stream("streams-plaintext-input")
            .flatMapValues(value -> Collections.singletonList(""));
        final Topology topology = builder.build();

        final TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);
        final TestInputTopic<String, String> topic = testDriver.createInputTopic("streams-plaintext-input", new StringSerializer(), new StringSerializer());

        final StreamsException se = assertThrows(StreamsException.class, () -> topic.pipeInput("a-key", "a value"));
        final String msg = se.getMessage();
        assertTrue("Error about class cast with serdes", msg.contains("ClassCastException"));
        assertTrue("Error about class cast with serdes", msg.contains("Serdes"));
    }

    private static class ClassCastProcessor extends ExceptionalProcessor {

        @Override
        public void init(final ProcessorContext context) {
        }

        @Override
        public void process(final Object key, final Object value) {
            throw new ClassCastException("Incompatible types simulation exception.");
        }
    }

    @Test
    public void testTopologyLevelClassCastExceptionDirect() {
        final Properties properties = StreamsTestUtils.getStreamsConfig();
        properties.put(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, StreamsConfig.METRICS_LATEST);
        final InternalProcessorContext<Object, Object> context = new MockInternalProcessorContext(properties, new Metrics());
        final ProcessorNode<Object, Object> node = new ProcessorNode<>("name", new ClassCastProcessor(), Collections.emptySet());
        node.init(context);
        final StreamsException se = assertThrows(StreamsException.class, () -> node.process("aKey", "aValue"));
        assertThat(se.getCause(), instanceOf(ClassCastException.class));
        assertThat(se.getMessage(), containsString("default Serdes"));
        assertThat(se.getMessage(), containsString("input types"));
    }
}
