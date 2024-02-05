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

    // Verify that when commitAsync is called but the supplied callback is not called by the consumer before a
    // rebalance occurs, the async callback does not reset the last committed offset from the rebalance.
    // See KAFKA-5731 for more information.
    @Test
    public void testCommitWithOutOfOrderCallback() throws Exception {
        createTask(initialState);

        expectInitializeTask();
        expectTaskGetTopic(true);

        // iter 1
        expectPollInitialAssignment();

        // iter 2
        expectConsumerPoll(1);
        expectConversionAndTransformation(4);
        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall();

        final Map<TopicPartition, OffsetAndMetadata> workerStartingOffsets = new HashMap<>();
        workerStartingOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET));
        workerStartingOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));

        final Map<TopicPartition, OffsetAndMetadata> workerCurrentOffsets = new HashMap<>();
        workerCurrentOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        workerCurrentOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));

        final List<TopicPartition> originalPartitions = new ArrayList<>(INITIAL_ASSIGNMENT);
        final List<TopicPartition> rebalancedPartitions = asList(TOPIC_PARTITION, TOPIC_PARTITION2, TOPIC_PARTITION3);
        final Map<TopicPartition, OffsetAndMetadata> rebalanceOffsets = new HashMap<>();
        rebalanceOffsets.put(TOPIC_PARTITION, workerCurrentOffsets.get(TOPIC_PARTITION));
        rebalanceOffsets.put(TOPIC_PARTITION2, workerCurrentOffsets.get(TOPIC_PARTITION2));
        rebalanceOffsets.put(TOPIC_PARTITION3, new OffsetAndMetadata(FIRST_OFFSET));

        final Map<TopicPartition, OffsetAndMetadata> postRebalanceCurrentOffsets = new HashMap<>();
        postRebalanceCurrentOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 3));
        postRebalanceCurrentOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));
        postRebalanceCurrentOffsets.put(TOPIC_PARTITION3, new OffsetAndMetadata(FIRST_OFFSET + 2));

        EasyMock.expect(consumer.assignment()).andReturn(new HashSet<>(originalPartitions)).times(2);

        // iter 3 - note that we return the current offset to indicate they should be committed
        sinkTask.preCommit(workerCurrentOffsets);
        EasyMock.expectLastCall().andReturn(workerCurrentOffsets);

        // We need to delay the result of trying to commit offsets to Kafka via the consumer.commitAsync
        // method. We do this so that we can test that the callback is not called until after the rebalance
        // changes the lastCommittedOffsets. To fake this for tests we have the commitAsync build a function
        // that will call the callback with the appropriate parameters, and we'll run that function later.
        final AtomicReference<Runnable> asyncCallbackRunner = new AtomicReference<>();
        final AtomicBoolean asyncCallbackRan = new AtomicBoolean();

        consumer.commitAsync(EasyMock.eq(workerCurrentOffsets), EasyMock.anyObject());
        EasyMock.expectLastCall().andAnswer(() -> {
            // Grab the arguments passed to the consumer.commitAsync method
            final Object[] args = EasyMock.getCurrentArguments();
            @SuppressWarnings("unchecked")
            final Map<TopicPartition, OffsetAndMetadata> offsets = (Map<TopicPartition, OffsetAndMetadata>) args[0];
            final OffsetCommitCallback callback = (OffsetCommitCallback) args[1];
            asyncCallbackRunner.set(() -> {
                callback.onComplete(offsets, null);
                asyncCallbackRan.set(true);
            });
            return null;
        });

        // Expect the next poll to discover and perform the rebalance, THEN complete the previous callback handler,
        // and then return one record for TP1 and one for TP3.
        final AtomicBoolean rebalanced = new AtomicBoolean();
        EasyMock.expect(consumer.poll(Duration.ofMillis(EasyMock.anyLong()))).andAnswer(
            () -> {
                // Rebalance always begins with revoking current partitions ...
                rebalanceListener.getValue().onPartitionsRevoked(originalPartitions);
                // Respond to the rebalance
                Map<TopicPartition, Long> offsets = new HashMap<>();
                offsets.put(TOPIC_PARTITION, rebalanceOffsets.get(TOPIC_PARTITION).offset());
                offsets.put(TOPIC_PARTITION2, rebalanceOffsets.get(TOPIC_PARTITION2).offset());
                offsets.put(TOPIC_PARTITION3, rebalanceOffsets.get(TOPIC_PARTITION3).offset());
                sinkTaskContext.getValue().offset(offsets);
                rebalanceListener.getValue().onPartitionsAssigned(rebalancedPartitions);
                rebalanced.set(true);

                // Run the previous async commit handler
                asyncCallbackRunner.get().run();

                 // And prep the two records to return
                long timestamp = RecordBatch.NO_TIMESTAMP;
                TimestampType timestampType = TimestampType.NO_TIMESTAMP_TYPE;
                List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
                records.add(new ConsumerRecord<>(TOPIC, PARTITION, FIRST_OFFSET + recordsReturnedTp1 + 1, timestamp, timestampType,
                    0, 0, RAW_KEY, RAW_VALUE, new RecordHeaders(), Optional.empty()));
                records.add(new ConsumerRecord<>(TOPIC, PARTITION3, FIRST_OFFSET + recordsReturnedTp3 + 1, timestamp, timestampType,
                    0, 0, RAW_KEY, RAW_VALUE, new RecordHeaders(), Optional.empty()));
                recordsReturnedTp1 += 1;
                recordsReturnedTp3 += 1;
                return new ConsumerRecords<>(Collections.singletonMap(new TopicPartition(TOPIC, PARTITION), records));
            });

        // onPartitionsRevoked
        sinkTask.preCommit(workerCurrentOffsets);
        EasyMock.expectLastCall().andReturn(workerCurrentOffsets);
        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall();
        sinkTask.close(new ArrayList<>(workerCurrentOffsets.keySet()));
        EasyMock.expectLastCall();
        consumer.commitSync(workerCurrentOffsets);
        EasyMock.expectLastCall();

        // onPartitionsAssigned - step 1
        final long offsetTp1 = rebalanceOffsets.get(TOPIC_PARTITION).offset();
        final long offsetTp2 = rebalanceOffsets.get(TOPIC_PARTITION2).offset();
        final long offsetTp3 = rebalanceOffsets.get(TOPIC_PARTITION3).offset();
        EasyMock.expect(consumer.position(TOPIC_PARTITION)).andReturn(offsetTp1);
        EasyMock.expect(consumer.position(TOPIC_PARTITION2)).andReturn(offsetTp2);
        EasyMock.expect(consumer.position(TOPIC_PARTITION3)).andReturn(offsetTp3);
        EasyMock.expect(consumer.assignment()).andReturn(new HashSet<>(rebalancedPartitions)).times(5);

        // onPartitionsAssigned - step 2
        sinkTask.open(EasyMock.eq(rebalancedPartitions));
        EasyMock.expectLastCall();

        // onPartitionsAssigned - step 3 rewind
        consumer.seek(TOPIC_PARTITION, offsetTp1);
        EasyMock.expectLastCall();
        consumer.seek(TOPIC_PARTITION2, offsetTp2);
        EasyMock.expectLastCall();
        consumer.seek(TOPIC_PARTITION3, offsetTp3);
        EasyMock.expectLastCall();

        // iter 4 - note that we return the current offset to indicate they should be committed
        sinkTask.preCommit(postRebalanceCurrentOffsets);
        EasyMock.expectLastCall().andReturn(postRebalanceCurrentOffsets);

        final Capture<OffsetCommitCallback> callback = EasyMock.newCapture();
        consumer.commitAsync(EasyMock.eq(postRebalanceCurrentOffsets), EasyMock.capture(callback));
        EasyMock.expectLastCall().andAnswer(() -> {
            callback.getValue().onComplete(postRebalanceCurrentOffsets, null);
            return null;
        });

        // no actual consumer.commit() triggered
        expectConsumerPoll(1);

        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration(); // iter 1 -- initial assignment

        assertEquals(workerStartingOffsets, Whitebox.getInternalState(workerTask, "currentOffsets"));
        assertEquals(workerStartingOffsets, Whitebox.getInternalState(workerTask, "lastCommittedOffsets"));

        time.sleep(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_DEFAULT);
        workerTask.iteration(); // iter 2 -- deliver 2 records

        sinkTaskContext.getValue().requestCommit();
        workerTask.iteration(); // iter 3 -- commit in progress

        assertSinkMetricValue("partition-count", 3);
        assertSinkMetricValue("sink-record-read-total", 3.0);
        assertSinkMetricValue("sink-record-send-total", 3.0);
        assertSinkMetricValue("sink-record-active-count", 4.0);
        assertSinkMetricValue("sink-record-active-count-max", 4.0);
        assertSinkMetricValue("sink-record-active-count-avg", 0.71429);
        assertSinkMetricValue("offset-commit-seq-no", 2.0);
        assertSinkMetricValue("offset-commit-completion-total", 1.0);
        assertSinkMetricValue("offset-commit-skip-total", 1.0);
        assertTaskMetricValue("status", "running");
        assertTaskMetricValue("running-ratio", 1.0);
        assertTaskMetricValue("pause-ratio", 0.0);
        assertTaskMetricValue("batch-size-max", 2.0);
        assertTaskMetricValue("batch-size-avg", 1.0);
        assertTaskMetricValue("offset-commit-max-time-ms", 0.0);
        assertTaskMetricValue("offset-commit-avg-time-ms", 0.0);
        assertTaskMetricValue("offset-commit-failure-percentage", 0.0);
        assertTaskMetricValue("offset-commit-success-percentage", 1.0);

        assertTrue(asyncCallbackRan.get());
        assertTrue(rebalanced.get());

        // Check that the offsets were not reset by the out-of-order async commit callback
        assertEquals(postRebalanceCurrentOffsets, Whitebox.getInternalState(workerTask, "currentOffsets"));
        assertEquals(rebalanceOffsets, Whitebox.getInternalState(workerTask, "lastCommittedOffsets"));

        time.sleep(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_DEFAULT);
        sinkTaskContext.getValue().requestCommit();
        workerTask.iteration(); // iter 4 -- commit in progress

        // Check that the offsets were not reset by the out-of-order async commit callback
        assertEquals(postRebalanceCurrentOffsets, Whitebox.getInternalState(workerTask, "currentOffsets"));
        assertEquals(postRebalanceCurrentOffsets, Whitebox.getInternalState(workerTask, "lastCommittedOffsets"));

        assertSinkMetricValue("partition-count", 3);
        assertSinkMetricValue("sink-record-read-total", 4.0);
        assertSinkMetricValue("sink-record-send-total", 4.0);
        assertSinkMetricValue("sink-record-active-count", 0.0);
        assertSinkMetricValue("sink-record-active-count-max", 4.0);
        assertSinkMetricValue("sink-record-active-count-avg", 0.5555555);
        assertSinkMetricValue("offset-commit-seq-no", 3.0);
        assertSinkMetricValue("offset-commit-completion-total", 2.0);
        assertSinkMetricValue("offset-commit-skip-total", 1.0);
        assertTaskMetricValue("status", "running");
        assertTaskMetricValue("running-ratio", 1.0);
        assertTaskMetricValue("pause-ratio", 0.0);
        assertTaskMetricValue("batch-size-max", 2.0);
        assertTaskMetricValue("batch-size-avg", 1.0);
        assertTaskMetricValue("offset-commit-max-time-ms", 0.0);
        assertTaskMetricValue("offset-commit-avg-time-ms", 0.0);
        assertTaskMetricValue("offset-commit-failure-percentage", 0.0);
        assertTaskMetricValue("offset-commit-success-percentage", 1.0);

        PowerMock.verifyAll();
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
