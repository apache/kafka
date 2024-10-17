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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.SensorAccessor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.internals.WrappingNullableUtils;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockSourceNode;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

public class SourceNodeTest {
    private MockedStatic<WrappingNullableUtils> utilsMock;

    @BeforeEach
    public void setup() {
        utilsMock = Mockito.mockStatic(WrappingNullableUtils.class);
    }

    @AfterEach
    public void cleanup() {
        utilsMock.close();
    }


    @Test
    public void shouldProvideTopicHeadersAndDataToKeyDeserializer() {
        final SourceNode<String, String> sourceNode = new MockSourceNode<>(new TheDeserializer(), new TheDeserializer());
        final RecordHeaders headers = new RecordHeaders();
        final String deserializeKey = sourceNode.deserializeKey("topic", headers, "data".getBytes(StandardCharsets.UTF_8));
        assertThat(deserializeKey, is("topic" + headers + "data"));
    }

    @Test
    public void shouldProvideTopicHeadersAndDataToValueDeserializer() {
        final SourceNode<String, String> sourceNode = new MockSourceNode<>(new TheDeserializer(), new TheDeserializer());
        final RecordHeaders headers = new RecordHeaders();
        final String deserializedValue = sourceNode.deserializeValue("topic", headers, "data".getBytes(StandardCharsets.UTF_8));
        assertThat(deserializedValue, is("topic" + headers + "data"));
    }

    public static class TheDeserializer implements Deserializer<String> {
        @Override
        public String deserialize(final String topic, final Headers headers, final byte[] data) {
            return topic + headers + new String(data, StandardCharsets.UTF_8);
        }

        @Override
        public String deserialize(final String topic, final byte[] data) {
            return deserialize(topic, null, data);
        }
    }

    @Test
    public void shouldExposeProcessMetrics() {
        final Metrics metrics = new Metrics();
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, "test-client", new MockTime());
        final InternalMockProcessorContext<String, String> context = new InternalMockProcessorContext<>(streamsMetrics);
        final SourceNode<String, String> node =
            new SourceNode<>(context.currentNode().name(), new TheDeserializer(), new TheDeserializer());
        node.init(context);

        final String threadId = Thread.currentThread().getName();
        final String groupName = "stream-processor-node-metrics";
        final Map<String, String> metricTags = mkMap(
            mkEntry("thread-id", threadId),
            mkEntry("task-id", context.taskId().toString()),
            mkEntry("processor-node-id", node.name())
        );

        assertTrue(StreamsTestUtils.containsMetric(metrics, "process-rate", groupName, metricTags));
        assertTrue(StreamsTestUtils.containsMetric(metrics, "process-total", groupName, metricTags));

        // test parent sensors
        final String parentGroupName = "stream-task-metrics";
        metricTags.remove("processor-node-id");
        assertTrue(StreamsTestUtils.containsMetric(metrics, "process-rate", parentGroupName, metricTags));
        assertTrue(StreamsTestUtils.containsMetric(metrics, "process-total", parentGroupName, metricTags));

        final String sensorNamePrefix = "internal." + threadId + ".task." + context.taskId().toString();
        final Sensor processSensor =
            metrics.getSensor(sensorNamePrefix + ".node." + context.currentNode().name() + ".s.process");
        final SensorAccessor sensorAccessor = new SensorAccessor(processSensor);
        assertThat(
            sensorAccessor.parents().stream().map(Sensor::name).collect(Collectors.toList()),
            contains(sensorNamePrefix + ".s.process")
        );
    }

    @Test
    public void shouldThrowStreamsExceptionOnUndefinedKeySerde() {
        final InternalMockProcessorContext<String, String> context = new InternalMockProcessorContext<>();

        final SourceNode<String, String> node =
            new SourceNode<>(context.currentNode().name(), new TheDeserializer(), new TheDeserializer());

        utilsMock.when(() -> WrappingNullableUtils.prepareKeyDeserializer(any(), any(), any()))
            .thenThrow(new ConfigException("Please set StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG"));

        final Throwable exception = assertThrows(StreamsException.class, () -> node.init(context));

        assertThat(
            exception.getMessage(),
            equalTo("Failed to initialize key serdes for source node TESTING_NODE")
        );
        assertThat(
            exception.getCause().getMessage(),
            equalTo("Please set StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG")
        );
    }

    @Test
    public void shouldThrowStreamsExceptionOnUndefinedValueSerde() {
        final InternalMockProcessorContext<String, String> context = new InternalMockProcessorContext<>();

        final SourceNode<String, String> node =
            new SourceNode<>(context.currentNode().name(), new TheDeserializer(), new TheDeserializer());

        utilsMock.when(() -> WrappingNullableUtils.prepareValueDeserializer(any(), any(), any()))
            .thenThrow(new ConfigException("Please set StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG"));

        final Throwable exception = assertThrows(StreamsException.class, () -> node.init(context));

        assertThat(
            exception.getMessage(),
            equalTo("Failed to initialize value serdes for source node TESTING_NODE")
        );
        assertThat(
            exception.getCause().getMessage(),
            equalTo("Please set StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG")
        );
    }

    @Test
    public void shouldThrowStreamsExceptionWithExplicitErrorMessage() {
        final InternalMockProcessorContext<String, String> context = new InternalMockProcessorContext<>();

        final SourceNode<String, String> node =
            new SourceNode<>(context.currentNode().name(), new TheDeserializer(), new TheDeserializer());

        utilsMock.when(() -> WrappingNullableUtils.prepareKeyDeserializer(any(), any(), any())).thenThrow(new StreamsException(""));

        final Throwable exception = assertThrows(StreamsException.class, () -> node.init(context));

        assertThat(exception.getMessage(), equalTo("Failed to initialize key serdes for source node TESTING_NODE"));
    }
}
