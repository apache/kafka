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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.ErrorReporter;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperatorTest;
import org.apache.kafka.connect.runtime.isolation.PluginClassLoader;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest(WorkerSinkTask.class)
@PowerMockIgnore("javax.management.*")
public class WorkerSinkTaskTest {
    // These are fixed to keep this code simpler. In this example we assume byte[] raw values
    // with mix of integer/string in Connect
    private static final String TOPIC = "test";
    private static final int PARTITION = 12;
    private static final int PARTITION2 = 13;
    private static final int PARTITION3 = 14;
    private static final long FIRST_OFFSET = 45;
    private static final Schema KEY_SCHEMA = Schema.INT32_SCHEMA;
    private static final int KEY = 12;
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;
    private static final String VALUE = "VALUE";
    private static final byte[] RAW_KEY = "key".getBytes();
    private static final byte[] RAW_VALUE = "value".getBytes();

    private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, PARTITION);
    private static final TopicPartition TOPIC_PARTITION2 = new TopicPartition(TOPIC, PARTITION2);
    private static final TopicPartition TOPIC_PARTITION3 = new TopicPartition(TOPIC, PARTITION3);

    private static final Set<TopicPartition> INITIAL_ASSIGNMENT =
        new HashSet<>(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2));

    private static final Map<String, String> TASK_PROPS = new HashMap<>();
    static {
        TASK_PROPS.put(SinkConnector.TOPICS_CONFIG, TOPIC);
        TASK_PROPS.put(TaskConfig.TASK_CLASS_CONFIG, TestSinkTask.class.getName());
    }
    private static final TaskConfig TASK_CONFIG = new TaskConfig(TASK_PROPS);

    private ConnectorTaskId taskId = new ConnectorTaskId("job", 0);
    private TargetState initialState = TargetState.STARTED;
    private MockTime time;
    private WorkerSinkTask workerTask;
    @Mock
    private SinkTask sinkTask;
    private Capture<WorkerSinkTaskContext> sinkTaskContext = EasyMock.newCapture();
    private WorkerConfig workerConfig;
    private MockConnectMetrics metrics;
    @Mock
    private PluginClassLoader pluginLoader;
    @Mock
    private Converter keyConverter;
    @Mock
    private Converter valueConverter;
    @Mock
    private HeaderConverter headerConverter;
    @Mock
    private TransformationChain<ConsumerRecord<byte[], byte[]>, SinkRecord> transformationChain;
    @Mock
    private TaskStatus.Listener statusListener;
    @Mock
    private StatusBackingStore statusBackingStore;
    @Mock
    private KafkaConsumer<byte[], byte[]> consumer;
    @Mock
    private ErrorHandlingMetrics errorHandlingMetrics;
    private Capture<ConsumerRebalanceListener> rebalanceListener = EasyMock.newCapture();

    private long recordsReturnedTp1;
    private long recordsReturnedTp3;

    @Before
    public void setUp() {
        time = new MockTime();
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("offset.storage.file.filename", "/tmp/connect.offsets");
        workerConfig = new StandaloneConfig(workerProps);
        pluginLoader = PowerMock.createMock(PluginClassLoader.class);
        metrics = new MockConnectMetrics(time);
        recordsReturnedTp1 = 0;
        recordsReturnedTp3 = 0;
    }

    private void createTask(TargetState initialState) {
        createTask(initialState, keyConverter, valueConverter, headerConverter);
    }

    private void createTask(TargetState initialState, Converter keyConverter, Converter valueConverter, HeaderConverter headerConverter) {
        createTask(initialState, keyConverter, valueConverter, headerConverter, RetryWithToleranceOperatorTest.noopOperator(), Collections::emptyList);
    }

    private void createTask(TargetState initialState, Converter keyConverter, Converter valueConverter, HeaderConverter headerConverter,
                            RetryWithToleranceOperator<ConsumerRecord<byte[], byte[]>> retryWithToleranceOperator, Supplier<List<ErrorReporter<ConsumerRecord<byte[], byte[]>>>> errorReportersSupplier) {
        workerTask = new WorkerSinkTask(
                taskId, sinkTask, statusListener, initialState, workerConfig, ClusterConfigState.EMPTY, metrics,
                keyConverter, valueConverter, errorHandlingMetrics, headerConverter,
                transformationChain, consumer, pluginLoader, time,
                retryWithToleranceOperator, null, statusBackingStore, errorReportersSupplier);
    }

    @After
    public void tearDown() {
        if (metrics != null) metrics.stop();
    }


    private void expectInitializeTask() {
        consumer.subscribe(EasyMock.eq(asList(TOPIC)), EasyMock.capture(rebalanceListener));
        PowerMock.expectLastCall();

        sinkTask.initialize(EasyMock.capture(sinkTaskContext));
        PowerMock.expectLastCall();
        sinkTask.start(TASK_PROPS);
        PowerMock.expectLastCall();
    }

    private void expectPollInitialAssignment() {
        sinkTask.open(INITIAL_ASSIGNMENT);
        EasyMock.expectLastCall();

        EasyMock.expect(consumer.assignment()).andReturn(INITIAL_ASSIGNMENT).times(2);

        EasyMock.expect(consumer.poll(Duration.ofMillis(EasyMock.anyLong()))).andAnswer(() -> {
            rebalanceListener.getValue().onPartitionsAssigned(INITIAL_ASSIGNMENT);
            return ConsumerRecords.empty();
        });
        INITIAL_ASSIGNMENT.forEach(tp -> EasyMock.expect(consumer.position(tp)).andReturn(FIRST_OFFSET));

        sinkTask.put(Collections.emptyList());
        EasyMock.expectLastCall();
    }

    private void expectConsumerPoll(final int numMessages) {
        expectConsumerPoll(numMessages, RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, emptyHeaders());
    }

    private void expectConsumerPoll(final int numMessages, final long timestamp, final TimestampType timestampType, Headers headers) {
        EasyMock.expect(consumer.poll(Duration.ofMillis(EasyMock.anyLong()))).andAnswer(
            () -> {
                List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
                for (int i = 0; i < numMessages; i++)
                    records.add(new ConsumerRecord<>(TOPIC, PARTITION, FIRST_OFFSET + recordsReturnedTp1 + i, timestamp, timestampType,
                        0, 0, RAW_KEY, RAW_VALUE, headers, Optional.empty()));
                recordsReturnedTp1 += numMessages;
                return new ConsumerRecords<>(
                        numMessages > 0 ?
                                Collections.singletonMap(new TopicPartition(TOPIC, PARTITION), records) :
                                Collections.emptyMap()
                );
            });
    }

    private void expectConversionAndTransformation(final int numMessages) {
        expectConversionAndTransformation(numMessages, null);
    }

    private void expectConversionAndTransformation(final int numMessages, final String topicPrefix) {
        expectConversionAndTransformation(numMessages, topicPrefix, emptyHeaders());
    }

    private void expectConversionAndTransformation(final int numMessages, final String topicPrefix, final Headers headers) {
        EasyMock.expect(keyConverter.toConnectData(TOPIC, headers, RAW_KEY)).andReturn(new SchemaAndValue(KEY_SCHEMA, KEY)).times(numMessages);
        EasyMock.expect(valueConverter.toConnectData(TOPIC, headers, RAW_VALUE)).andReturn(new SchemaAndValue(VALUE_SCHEMA, VALUE)).times(numMessages);

        for (Header header : headers) {
            EasyMock.expect(headerConverter.toConnectHeader(TOPIC, header.key(), header.value())).andReturn(new SchemaAndValue(VALUE_SCHEMA, new String(header.value()))).times(1);
        }

        expectTransformation(numMessages, topicPrefix);
    }

    private void expectTransformation(final int numMessages, final String topicPrefix) {
        final Capture<SinkRecord> recordCapture = EasyMock.newCapture();
        EasyMock.expect(transformationChain.apply(EasyMock.anyObject(), EasyMock.capture(recordCapture)))
                .andAnswer(() -> {
                    SinkRecord origRecord = recordCapture.getValue();
                    return topicPrefix != null && !topicPrefix.isEmpty()
                           ? origRecord.newRecord(
                                   topicPrefix + origRecord.topic(),
                                   origRecord.kafkaPartition(),
                                   origRecord.keySchema(),
                                   origRecord.key(),
                                   origRecord.valueSchema(),
                                   origRecord.value(),
                                   origRecord.timestamp(),
                                   origRecord.headers()
                           )
                           : origRecord;
                }).times(numMessages);
    }

    private void expectTaskGetTopic(boolean anyTimes) {
        final Capture<String> connectorCapture = EasyMock.newCapture();
        final Capture<String> topicCapture = EasyMock.newCapture();
        IExpectationSetters<TopicStatus> expect = EasyMock.expect(statusBackingStore.getTopic(
                EasyMock.capture(connectorCapture),
                EasyMock.capture(topicCapture)));
        if (anyTimes) {
            expect.andStubAnswer(() -> new TopicStatus(
                    topicCapture.getValue(),
                    new ConnectorTaskId(connectorCapture.getValue(), 0),
                    Time.SYSTEM.milliseconds()));
        } else {
            expect.andAnswer(() -> new TopicStatus(
                    topicCapture.getValue(),
                    new ConnectorTaskId(connectorCapture.getValue(), 0),
                    Time.SYSTEM.milliseconds()));
        }
        if (connectorCapture.hasCaptured() && topicCapture.hasCaptured()) {
            assertEquals("job", connectorCapture.getValue());
            assertEquals(TOPIC, topicCapture.getValue());
        }
    }

    private void assertSinkMetricValue(String name, double expected) {
        MetricGroup sinkTaskGroup = workerTask.sinkTaskMetricsGroup().metricGroup();
        double measured = metrics.currentMetricValueAsDouble(sinkTaskGroup, name);
        assertEquals(expected, measured, 0.001d);
    }

    private void assertTaskMetricValue(String name, double expected) {
        MetricGroup taskGroup = workerTask.taskMetricsGroup().metricGroup();
        double measured = metrics.currentMetricValueAsDouble(taskGroup, name);
        assertEquals(expected, measured, 0.001d);
    }

    private void assertTaskMetricValue(String name, String expected) {
        MetricGroup taskGroup = workerTask.taskMetricsGroup().metricGroup();
        String measured = metrics.currentMetricValueAsString(taskGroup, name);
        assertEquals(expected, measured);
    }

    private RecordHeaders emptyHeaders() {
        return new RecordHeaders();
    }

    private abstract static class TestSinkTask extends SinkTask  {
    }
}
