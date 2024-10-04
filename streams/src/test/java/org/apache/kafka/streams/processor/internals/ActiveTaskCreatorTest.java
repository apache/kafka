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

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.test.MockClientSupplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class ActiveTaskCreatorTest {

    @Mock
    private InternalTopologyBuilder builder;
    @Mock
    private StateDirectory stateDirectory;
    @Mock
    private ChangelogReader changeLogReader;

    private final MockClientSupplier mockClientSupplier = new MockClientSupplier();
    private final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), "clientId", new MockTime());
    private final Map<String, Object> properties = mkMap(
        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
    );
    final UUID uuid = UUID.randomUUID();

    private ActiveTaskCreator activeTaskCreator;

    // non-EOS test

    // functional test

    @Test
    public void shouldConstructProducerMetricsWithEosDisabled() {
        shouldConstructThreadProducerMetric();
    }

    @Test
    public void shouldConstructClientIdWithEosDisabled() {
        createTasks();

        final String clientIds = activeTaskCreator.producerClientIds();

        assertThat(clientIds, is("clientId-StreamThread-0-producer"));
    }

    @Test
    public void shouldCloseThreadProducerIfEosDisabled() {
        createTasks();

        activeTaskCreator.closeThreadProducerIfNeeded();

        assertThat(mockClientSupplier.producers.get(0).closed(), is(true));
    }

    @Test
    public void shouldNoOpCloseTaskProducerIfEosDisabled() {
        createTasks();

        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(new TaskId(0, 0));
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(new TaskId(0, 1));

        assertThat(mockClientSupplier.producers.get(0).closed(), is(false));
    }

    @Test
    public void shouldReturnBlockedTimeWhenThreadProducer() {
        final double blockedTime = 123.0;
        createTasks();
        final MockProducer<?, ?> producer = mockClientSupplier.producers.get(0);
        addMetric(producer, "flush-time-ns-total", blockedTime);

        assertThat(activeTaskCreator.totalProducerBlockedTime(), closeTo(blockedTime, 0.01));
    }

    // error handling

    @Test
    public void shouldFailOnStreamsProducerPerTaskIfEosDisabled() {
        createTasks();

        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> activeTaskCreator.streamsProducerForTask(null)
        );

        assertThat(thrown.getMessage(), is("Expected EXACTLY_ONCE to be enabled, but the processing mode was AT_LEAST_ONCE"));
    }

    @Test
    public void shouldReturnThreadProducerIfAtLeastOnceIsEnabled() {
        createTasks();

        final StreamsProducer threadProducer = activeTaskCreator.threadProducer();

        assertThat(mockClientSupplier.producers.size(), is(1));
        assertThat(threadProducer.kafkaProducer(), is(mockClientSupplier.producers.get(0)));
    }

    @Test
    public void shouldThrowStreamsExceptionOnErrorCloseThreadProducerIfEosDisabled() {
        createTasks();
        mockClientSupplier.producers.get(0).closeException = new RuntimeException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            activeTaskCreator::closeThreadProducerIfNeeded
        );

        assertThat(thrown.getMessage(), is("Thread producer encounter error trying to close."));
        assertThat(thrown.getCause().getMessage(), is("KABOOM!"));
    }



    // eos-v2 test

    // functional test

    @Test
    public void shouldReturnThreadProducerIfEosV2Enabled() {
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        mockClientSupplier.setApplicationIdForProducer("appId");

        createTasks();

        final StreamsProducer threadProducer = activeTaskCreator.threadProducer();

        assertThat(mockClientSupplier.producers.size(), is(1));
        assertThat(threadProducer.kafkaProducer(), is(mockClientSupplier.producers.get(0)));
    }

    @Test
    public void shouldConstructProducerMetricsWithEosV2Enabled() {
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        mockClientSupplier.setApplicationIdForProducer("appId");

        shouldConstructThreadProducerMetric();
    }

    @Test
    public void shouldConstructClientIdWithEosV2Enabled() {
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        mockClientSupplier.setApplicationIdForProducer("appId");
        createTasks();

        final String clientIds = activeTaskCreator.producerClientIds();

        assertThat(clientIds, is("clientId-StreamThread-0-producer"));
    }

    @Test
    public void shouldCloseThreadProducerIfEosV2Enabled() {
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        mockClientSupplier.setApplicationIdForProducer("appId");
        createTasks();

        activeTaskCreator.closeThreadProducerIfNeeded();

        assertThat(mockClientSupplier.producers.get(0).closed(), is(true));
    }

    @Test
    public void shouldNoOpCloseTaskProducerIfEosV2Enabled() {
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        mockClientSupplier.setApplicationIdForProducer("appId");

        createTasks();

        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(new TaskId(0, 0));
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(new TaskId(0, 1));

        assertThat(mockClientSupplier.producers.get(0).closed(), is(false));
    }

    // error handling

    @Test
    public void shouldFailOnStreamsProducerPerTaskIfEosV2Enabled() {
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        mockClientSupplier.setApplicationIdForProducer("appId");

        createTasks();

        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> activeTaskCreator.streamsProducerForTask(null)
        );

        assertThat(thrown.getMessage(), is("Expected EXACTLY_ONCE to be enabled, but the processing mode was EXACTLY_ONCE_V2"));
    }

    @Test
    public void shouldThrowStreamsExceptionOnErrorCloseThreadProducerIfEosV2Enabled() {
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        mockClientSupplier.setApplicationIdForProducer("appId");
        createTasks();
        mockClientSupplier.producers.get(0).closeException = new RuntimeException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            activeTaskCreator::closeThreadProducerIfNeeded
        );

        assertThat(thrown.getMessage(), is("Thread producer encounter error trying to close."));
        assertThat(thrown.getCause().getMessage(), is("KABOOM!"));
    }

    private void shouldConstructThreadProducerMetric() {
        createTasks();

        final MetricName testMetricName = new MetricName("test_metric", "", "", new HashMap<>());
        final Metric testMetric = new KafkaMetric(
            new Object(),
            testMetricName,
            (Measurable) (config, now) -> 0,
            null,
            new MockTime());
        mockClientSupplier.producers.get(0).setMockMetrics(testMetricName, testMetric);
        assertThat(mockClientSupplier.producers.size(), is(1));

        final Map<MetricName, Metric> producerMetrics = activeTaskCreator.producerMetrics();

        assertThat(producerMetrics.size(), is(1));
        assertThat(producerMetrics.get(testMetricName), is(testMetric));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void createTasks() {
        final TaskId task00 = new TaskId(0, 0);
        final TaskId task01 = new TaskId(0, 1);

        final ProcessorTopology topology = mock(ProcessorTopology.class);
        final SourceNode sourceNode = mock(SourceNode.class);

        when(builder.topologyConfigs()).thenReturn(new TopologyConfig(new StreamsConfig(properties)));
        when(builder.buildSubtopology(0)).thenReturn(topology);
        when(topology.sinkTopics()).thenReturn(emptySet());
        when(stateDirectory.getOrCreateDirectoryForTask(task00)).thenReturn(mock(File.class));
        when(stateDirectory.checkpointFileFor(task00)).thenReturn(mock(File.class));
        when(stateDirectory.getOrCreateDirectoryForTask(task01)).thenReturn(mock(File.class));
        when(stateDirectory.checkpointFileFor(task01)).thenReturn(mock(File.class));
        when(topology.source("topic")).thenReturn(sourceNode);
        when(sourceNode.timestampExtractor()).thenReturn(mock(TimestampExtractor.class));
        when(topology.sources()).thenReturn(Collections.singleton(sourceNode));

        final StreamsConfig config = new StreamsConfig(properties);
        activeTaskCreator = new ActiveTaskCreator(
            new TopologyMetadata(builder, config),
            config,
            streamsMetrics,
            stateDirectory,
            changeLogReader,
            new ThreadCache(new LogContext(), 0L, streamsMetrics),
            new MockTime(),
            mockClientSupplier,
            "clientId-StreamThread-0",
            0,
            uuid,
            new LogContext().logger(ActiveTaskCreator.class),
            false,
            false);

        assertThat(
            activeTaskCreator.createTasks(
                mockClientSupplier.consumer,
                mkMap(
                    mkEntry(task00, Collections.singleton(new TopicPartition("topic", 0))),
                    mkEntry(task01, Collections.singleton(new TopicPartition("topic", 1)))
                )
            ).stream().map(Task::id).collect(Collectors.toSet()),
            equalTo(mkSet(task00, task01))
        );
    }

    private void addMetric(
        final MockProducer<?, ?> producer,
        final String name,
        final double value) {
        final MetricName metricName = metricName(name);
        producer.setMockMetrics(metricName, new Metric() {
            @Override
            public MetricName metricName() {
                return metricName;
            }

            @Override
            public Object metricValue() {
                return value;
            }
        });
    }

    private MetricName metricName(final String name) {
        return new MetricName(name, "", "", Collections.emptyMap());
    }
}
