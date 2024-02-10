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
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.WorkerSinkTask.SinkTaskMetricsGroup;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.ErrorReporter;
import org.apache.kafka.connect.runtime.errors.ProcessingContext;
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
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class WorkerSinkTaskMockitoTest {
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
        TASK_PROPS.put(TaskConfig.TASK_CLASS_CONFIG, SinkTask.class.getName());
    }

    private static final TaskConfig TASK_CONFIG = new TaskConfig(TASK_PROPS);

    private ConnectorTaskId taskId = new ConnectorTaskId("job", 0);
    private ConnectorTaskId taskId1 = new ConnectorTaskId("job", 1);
    private TargetState initialState = TargetState.STARTED;
    private MockTime time;
    private WorkerSinkTask workerTask;
    @Mock
    private SinkTask sinkTask;
    private ArgumentCaptor<WorkerSinkTaskContext> sinkTaskContext = ArgumentCaptor.forClass(WorkerSinkTaskContext.class);
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
    private ArgumentCaptor<ConsumerRebalanceListener> rebalanceListener = ArgumentCaptor.forClass(ConsumerRebalanceListener.class);
    @Rule
    public final MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

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
                            RetryWithToleranceOperator<ConsumerRecord<byte[], byte[]>> retryWithToleranceOperator,
                            Supplier<List<ErrorReporter<ConsumerRecord<byte[], byte[]>>>> errorReportersSupplier) {
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

    @Test
    public void testStartPaused() {
        createTask(TargetState.PAUSED);

        expectPollInitialAssignment();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        workerTask.iteration();
        verifyPollInitialAssignment();

        time.sleep(10000L);
        verify(consumer).pause(INITIAL_ASSIGNMENT);

        assertSinkMetricValue("partition-count", 2);
        assertTaskMetricValue("status", "paused");
        assertTaskMetricValue("running-ratio", 0.0);
        assertTaskMetricValue("pause-ratio", 1.0);
        assertTaskMetricValue("offset-commit-max-time-ms", Double.NaN);
    }

    @Test
    public void testPause() {
        createTask(initialState);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        expectTaskGetTopic();
        expectPollInitialAssignment()
                .thenAnswer(expectConsumerPoll(1))
                // Pause
                .thenThrow(new WakeupException())
                // Offset commit as requested when pausing; No records returned by consumer.poll()
                .thenAnswer(expectConsumerPoll(0))
                // And unpause
                .thenThrow(new WakeupException())
                .thenAnswer(expectConsumerPoll(1));

        expectConversionAndTransformation(null, new RecordHeaders());

        workerTask.iteration(); // initial assignment
        verifyPollInitialAssignment();

        workerTask.iteration(); // fetch some data
        // put should've been called twice now (initial assignment & poll)
        verify(sinkTask, times(2)).put(anyList());

        workerTask.transitionTo(TargetState.PAUSED);
        time.sleep(10_000L);

        assertSinkMetricValue("partition-count", 2);
        assertSinkMetricValue("sink-record-read-total", 1.0);
        assertSinkMetricValue("sink-record-send-total", 1.0);
        assertSinkMetricValue("sink-record-active-count", 1.0);
        assertSinkMetricValue("sink-record-active-count-max", 1.0);
        assertSinkMetricValue("sink-record-active-count-avg", 0.333333);
        assertSinkMetricValue("offset-commit-seq-no", 0.0);
        assertSinkMetricValue("offset-commit-completion-rate", 0.0);
        assertSinkMetricValue("offset-commit-completion-total", 0.0);
        assertSinkMetricValue("offset-commit-skip-rate", 0.0);
        assertSinkMetricValue("offset-commit-skip-total", 0.0);
        assertTaskMetricValue("status", "running");
        assertTaskMetricValue("running-ratio", 1.0);
        assertTaskMetricValue("pause-ratio", 0.0);
        assertTaskMetricValue("batch-size-max", 1.0);
        assertTaskMetricValue("batch-size-avg", 0.5);
        assertTaskMetricValue("offset-commit-max-time-ms", Double.NaN);
        assertTaskMetricValue("offset-commit-failure-percentage", 0.0);
        assertTaskMetricValue("offset-commit-success-percentage", 0.0);

        workerTask.iteration(); // wakeup
        // Pause
        verify(statusListener).onPause(taskId);
        verify(consumer).pause(INITIAL_ASSIGNMENT);
        verify(consumer).wakeup();

        // Offset commit as requested when pausing; No records returned by consumer.poll()
        when(sinkTask.preCommit(anyMap())).thenReturn(Collections.emptyMap());

        workerTask.iteration(); // now paused
        time.sleep(30000L);

        assertSinkMetricValue("offset-commit-seq-no", 1.0);
        assertSinkMetricValue("offset-commit-completion-rate", 0.0333);
        assertSinkMetricValue("offset-commit-completion-total", 1.0);
        assertSinkMetricValue("offset-commit-skip-rate", 0.0);
        assertSinkMetricValue("offset-commit-skip-total", 0.0);
        assertTaskMetricValue("status", "paused");
        assertTaskMetricValue("running-ratio", 0.25);
        assertTaskMetricValue("pause-ratio", 0.75);
        verify(sinkTask, times(3)).put(anyList());

        workerTask.transitionTo(TargetState.STARTED);
        workerTask.iteration(); // wakeup
        workerTask.iteration(); // now unpaused

        // And unpause
        verify(statusListener).onResume(taskId);
        verify(consumer, times(2)).wakeup();
        INITIAL_ASSIGNMENT.forEach(tp -> {
            verify(consumer).resume(singleton(tp));
        });
        verify(sinkTask, times(4)).put(anyList());
    }

    @Test
    public void testShutdown() throws Exception {
        createTask(initialState);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        expectTaskGetTopic();
        expectPollInitialAssignment()
                .thenAnswer(expectConsumerPoll(1));

        expectConversionAndTransformation(null, new RecordHeaders());

        workerTask.iteration();
        verifyPollInitialAssignment();
        sinkTaskContext.getValue().requestCommit(); // Force an offset commit

        // second iteration
        when(sinkTask.preCommit(anyMap())).thenReturn(Collections.emptyMap());

        workerTask.iteration();
        verify(sinkTask, times(2)).put(anyList());

        doAnswer((Answer<ConsumerRecords<byte[], byte[]>>) invocation -> {
            rebalanceListener.getValue().onPartitionsRevoked(INITIAL_ASSIGNMENT);
            return null;
        }).when(consumer).close();

        workerTask.stop();
        verify(consumer).wakeup();

        workerTask.close();
        verify(sinkTask).stop();
        verify(consumer).close();
        verify(headerConverter).close();
    }

    @Test
    public void testPollRedelivery() {
        createTask(initialState);
        expectTaskGetTopic();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        expectPollInitialAssignment()
                // If a retriable exception is thrown, we should redeliver the same batch, pausing the consumer in the meantime
                .thenAnswer(expectConsumerPoll(1))
                // Retry delivery should succeed
                .thenAnswer(expectConsumerPoll(0))
                .thenAnswer(expectConsumerPoll(0));
        expectConversionAndTransformation(null, new RecordHeaders());

        doAnswer(invocation -> null)
                .doThrow(new RetriableException("retry"))
                .doAnswer(invocation -> null)
                .when(sinkTask).put(anyList());

        workerTask.iteration();
        time.sleep(10000L);

        verifyPollInitialAssignment();
        verify(sinkTask).put(anyList());

        assertSinkMetricValue("partition-count", 2);
        assertSinkMetricValue("sink-record-read-total", 0.0);
        assertSinkMetricValue("sink-record-send-total", 0.0);
        assertSinkMetricValue("sink-record-active-count", 0.0);
        assertSinkMetricValue("sink-record-active-count-max", 0.0);
        assertSinkMetricValue("sink-record-active-count-avg", 0.0);
        assertSinkMetricValue("offset-commit-seq-no", 0.0);
        assertSinkMetricValue("offset-commit-completion-rate", 0.0);
        assertSinkMetricValue("offset-commit-completion-total", 0.0);
        assertSinkMetricValue("offset-commit-skip-rate", 0.0);
        assertSinkMetricValue("offset-commit-skip-total", 0.0);
        assertTaskMetricValue("status", "running");
        assertTaskMetricValue("running-ratio", 1.0);
        assertTaskMetricValue("pause-ratio", 0.0);
        assertTaskMetricValue("batch-size-max", 0.0);
        assertTaskMetricValue("batch-size-avg", 0.0);
        assertTaskMetricValue("offset-commit-max-time-ms", Double.NaN);
        assertTaskMetricValue("offset-commit-failure-percentage", 0.0);
        assertTaskMetricValue("offset-commit-success-percentage", 0.0);

        // Pause
        workerTask.iteration();

        verify(consumer, times(3)).assignment();
        verify(consumer).pause(INITIAL_ASSIGNMENT);

        // Retry delivery should succeed
        workerTask.iteration();
        time.sleep(30000L);

        verify(sinkTask, times(3)).put(anyList());
        INITIAL_ASSIGNMENT.forEach(tp -> {
            verify(consumer).resume(Collections.singleton(tp));
        });

        assertSinkMetricValue("sink-record-read-total", 1.0);
        assertSinkMetricValue("sink-record-send-total", 1.0);
        assertSinkMetricValue("sink-record-active-count", 1.0);
        assertSinkMetricValue("sink-record-active-count-max", 1.0);
        assertSinkMetricValue("sink-record-active-count-avg", 0.5);
        assertTaskMetricValue("status", "running");
        assertTaskMetricValue("running-ratio", 1.0);
        assertTaskMetricValue("batch-size-max", 1.0);
        assertTaskMetricValue("batch-size-avg", 0.5);

        // Expect commit
        final Map<TopicPartition, OffsetAndMetadata> workerCurrentOffsets = new HashMap<>();
        // Commit advance by one
        workerCurrentOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        // Nothing polled for this partition
        workerCurrentOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));
        when(sinkTask.preCommit(workerCurrentOffsets)).thenReturn(workerCurrentOffsets);

        sinkTaskContext.getValue().requestCommit();
        time.sleep(10000L);
        workerTask.iteration();

        final ArgumentCaptor<OffsetCommitCallback> callback = ArgumentCaptor.forClass(OffsetCommitCallback.class);
        verify(consumer).commitAsync(eq(workerCurrentOffsets), callback.capture());
        callback.getValue().onComplete(workerCurrentOffsets, null);

        verify(sinkTask, times(4)).put(anyList());
        assertSinkMetricValue("offset-commit-completion-total", 1.0);
    }

    @Test
    public void testErrorInRebalancePartitionLoss() {
        RuntimeException exception = new RuntimeException("Revocation error");
        createTask(initialState);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        expectPollInitialAssignment()
                .thenAnswer((Answer<ConsumerRecords<byte[], byte[]>>) invocation -> {
                    rebalanceListener.getValue().onPartitionsLost(INITIAL_ASSIGNMENT);
                    return ConsumerRecords.empty();
                });

        doThrow(exception).when(sinkTask).close(INITIAL_ASSIGNMENT);

        workerTask.iteration();
        verifyPollInitialAssignment();

        try {
            workerTask.iteration();
            fail("Poll should have raised the rebalance exception");
        } catch (RuntimeException e) {
            assertEquals(exception, e);
        }
    }

    @Test
    public void testErrorInRebalancePartitionRevocation() {
        RuntimeException exception = new RuntimeException("Revocation error");
        createTask(initialState);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        expectPollInitialAssignment()
                .thenAnswer((Answer<ConsumerRecords<byte[], byte[]>>) invocation -> {
                    rebalanceListener.getValue().onPartitionsRevoked(INITIAL_ASSIGNMENT);
                    return ConsumerRecords.empty();
                });

        expectRebalanceRevocationError(exception);

        workerTask.iteration();
        verifyPollInitialAssignment();
        try {
            workerTask.iteration();
            fail("Poll should have raised the rebalance exception");
        } catch (RuntimeException e) {
            assertEquals(exception, e);
        }
    }

    @Test
    public void testErrorInRebalancePartitionAssignment() {
        RuntimeException exception = new RuntimeException("Assignment error");
        createTask(initialState);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        expectPollInitialAssignment()
                .thenAnswer((Answer<ConsumerRecords<byte[], byte[]>>) invocation -> {
                    rebalanceListener.getValue().onPartitionsRevoked(INITIAL_ASSIGNMENT);
                    rebalanceListener.getValue().onPartitionsAssigned(INITIAL_ASSIGNMENT);
                    return ConsumerRecords.empty();
                });

        workerTask.iteration();
        verifyPollInitialAssignment();

        expectRebalanceAssignmentError(exception);
        try {
            workerTask.iteration();
            fail("Poll should have raised the rebalance exception");
        } catch (RuntimeException e) {
            assertEquals(exception, e);
        } finally {
            verify(sinkTask).close(INITIAL_ASSIGNMENT);
        }
    }

    @Test
    public void testPartialRevocationAndAssignment() {
        createTask(initialState);

        when(consumer.assignment())
                .thenReturn(INITIAL_ASSIGNMENT)
                .thenReturn(INITIAL_ASSIGNMENT)
                .thenReturn(Collections.singleton(TOPIC_PARTITION2))
                .thenReturn(Collections.singleton(TOPIC_PARTITION2))
                .thenReturn(new HashSet<>(Arrays.asList(TOPIC_PARTITION2, TOPIC_PARTITION3)))
                .thenReturn(new HashSet<>(Arrays.asList(TOPIC_PARTITION2, TOPIC_PARTITION3)))
                .thenReturn(INITIAL_ASSIGNMENT)
                .thenReturn(INITIAL_ASSIGNMENT)
                .thenReturn(INITIAL_ASSIGNMENT);

        INITIAL_ASSIGNMENT.forEach(tp -> when(consumer.position(tp)).thenReturn(FIRST_OFFSET));

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        when(consumer.poll(any(Duration.class)))
                .thenAnswer((Answer<ConsumerRecords<byte[], byte[]>>) invocation -> {
                    rebalanceListener.getValue().onPartitionsAssigned(INITIAL_ASSIGNMENT);
                    return ConsumerRecords.empty();
                })
                .thenAnswer((Answer<ConsumerRecords<byte[], byte[]>>) invocation -> {
                    rebalanceListener.getValue().onPartitionsRevoked(singleton(TOPIC_PARTITION));
                    rebalanceListener.getValue().onPartitionsAssigned(Collections.emptySet());
                    return ConsumerRecords.empty();
                })
                .thenAnswer((Answer<ConsumerRecords<byte[], byte[]>>) invocation -> {
                    rebalanceListener.getValue().onPartitionsRevoked(Collections.emptySet());
                    rebalanceListener.getValue().onPartitionsAssigned(singleton(TOPIC_PARTITION3));
                    return ConsumerRecords.empty();
                })
                .thenAnswer((Answer<ConsumerRecords<byte[], byte[]>>) invocation -> {
                    rebalanceListener.getValue().onPartitionsLost(singleton(TOPIC_PARTITION3));
                    rebalanceListener.getValue().onPartitionsAssigned(singleton(TOPIC_PARTITION));
                    return ConsumerRecords.empty();
                });

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET));
        when(sinkTask.preCommit(offsets)).thenReturn(offsets);

        when(consumer.position(TOPIC_PARTITION3)).thenReturn(FIRST_OFFSET);

        // First iteration--first call to poll, first consumer assignment
        workerTask.iteration();
        verifyPollInitialAssignment();

        // Second iteration--second call to poll, partial consumer revocation
        workerTask.iteration();
        verify(sinkTask).close(singleton(TOPIC_PARTITION));
        verify(sinkTask, times(2)).put(Collections.emptyList());

        // Third iteration--third call to poll, partial consumer assignment
        workerTask.iteration();
        verify(sinkTask).open(singleton(TOPIC_PARTITION3));
        verify(sinkTask, times(3)).put(Collections.emptyList());

        // Fourth iteration--fourth call to poll, one partition lost; can't commit offsets for it, one new partition assigned
        workerTask.iteration();
        verify(sinkTask).close(singleton(TOPIC_PARTITION3));
        verify(sinkTask).open(singleton(TOPIC_PARTITION));
        verify(sinkTask, times(4)).put(Collections.emptyList());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testTaskCancelPreventsFinalOffsetCommit() {
        createTask(initialState);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        expectTaskGetTopic();
        expectPollInitialAssignment()
                // Put one message through the task to get some offsets to commit
                .thenAnswer(expectConsumerPoll(1))
                // the second put will return after the task is stopped and cancelled (asynchronously)
                .thenAnswer(expectConsumerPoll(1));

        expectConversionAndTransformation(null, new RecordHeaders());

        doAnswer(invocation -> null)
                .doAnswer(invocation -> null)
                .doAnswer(invocation -> {
                    workerTask.stop();
                    workerTask.cancel();
                    return null;
                })
                .when(sinkTask).put(anyList());

        // task performs normal steps in advance of committing offsets
        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 2));
        offsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));
        when(sinkTask.preCommit(offsets)).thenReturn(offsets);

        workerTask.execute();

        // stop wakes up the consumer
        verify(consumer).wakeup();

        verify(sinkTask).close(any());
    }

    @Test
    public void testDeliveryWithMutatingTransform() {
        createTask(initialState);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        expectTaskGetTopic();
        expectPollInitialAssignment()
                .thenAnswer(expectConsumerPoll(1))
                .thenAnswer(expectConsumerPoll(0));

        expectConversionAndTransformation("newtopic_", new RecordHeaders());

        workerTask.iteration(); // initial assignment

        workerTask.iteration(); // first record delivered

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        offsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));
        when(sinkTask.preCommit(offsets)).thenReturn(offsets);

        sinkTaskContext.getValue().requestCommit();
        assertTrue(sinkTaskContext.getValue().isCommitRequested());

        assertNotEquals(offsets, workerTask.lastCommittedOffsets());
        workerTask.iteration(); // triggers the commit

        ArgumentCaptor<OffsetCommitCallback> callback = ArgumentCaptor.forClass(OffsetCommitCallback.class);
        verify(consumer).commitAsync(eq(offsets), callback.capture());

        callback.getValue().onComplete(offsets, null);

        assertFalse(sinkTaskContext.getValue().isCommitRequested()); // should have been cleared
        assertEquals(offsets, workerTask.lastCommittedOffsets());
        assertEquals(0, workerTask.commitFailures());
        assertEquals(1.0, metrics.currentMetricValueAsDouble(workerTask.taskMetricsGroup().metricGroup(), "batch-size-max"), 0.0001);
    }

    @Test
    public void testMissingTimestampPropagation() {
        createTask(initialState);
        expectTaskGetTopic();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        expectPollInitialAssignment()
                .thenAnswer(expectConsumerPoll(1, RecordBatch.NO_TIMESTAMP, TimestampType.CREATE_TIME, new RecordHeaders()));

        expectConversionAndTransformation(null, new RecordHeaders());

        workerTask.iteration(); // iter 1 -- initial assignment
        workerTask.iteration(); // iter 2 -- deliver 1 record

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<SinkRecord>> records = ArgumentCaptor.forClass(Collection.class);
        verify(sinkTask, times(2)).put(records.capture());

        SinkRecord record = records.getValue().iterator().next();

        // we expect null for missing timestamp, the sentinel value of Record.NO_TIMESTAMP is Kafka's API
        assertNull(record.timestamp());
        assertEquals(TimestampType.CREATE_TIME, record.timestampType());
    }

    @Test
    public void testTimestampPropagation() {
        final Long timestamp = System.currentTimeMillis();
        final TimestampType timestampType = TimestampType.CREATE_TIME;

        createTask(initialState);
        expectTaskGetTopic();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        expectPollInitialAssignment()
                .thenAnswer(expectConsumerPoll(1, timestamp, timestampType, new RecordHeaders()));

        expectConversionAndTransformation(null, new RecordHeaders());

        workerTask.iteration(); // iter 1 -- initial assignment
        workerTask.iteration(); // iter 2 -- deliver 1 record

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<SinkRecord>> records = ArgumentCaptor.forClass(Collection.class);
        verify(sinkTask, times(2)).put(records.capture());

        SinkRecord record = records.getValue().iterator().next();

        assertEquals(timestamp, record.timestamp());
        assertEquals(timestampType, record.timestampType());
    }

    @Test
    public void testTopicsRegex() {
        Map<String, String> props = new HashMap<>(TASK_PROPS);
        props.remove("topics");
        props.put("topics.regex", "te.*");
        TaskConfig taskConfig = new TaskConfig(props);

        createTask(TargetState.PAUSED);

        workerTask.initialize(taskConfig);
        workerTask.initializeAndStart();

        ArgumentCaptor<Pattern> topicsRegex = ArgumentCaptor.forClass(Pattern.class);

        verify(consumer).subscribe(topicsRegex.capture(), rebalanceListener.capture());
        assertEquals("te.*", topicsRegex.getValue().pattern());
        verify(sinkTask).initialize(sinkTaskContext.capture());
        verify(sinkTask).start(props);

        expectPollInitialAssignment();

        workerTask.iteration();
        time.sleep(10000L);

        verify(consumer).pause(INITIAL_ASSIGNMENT);
    }

    @Test
    public void testMetricsGroup() {
        SinkTaskMetricsGroup group = new SinkTaskMetricsGroup(taskId, metrics);
        SinkTaskMetricsGroup group1 = new SinkTaskMetricsGroup(taskId1, metrics);
        for (int i = 0; i != 10; ++i) {
            group.recordRead(1);
            group.recordSend(2);
            group.recordPut(3);
            group.recordPartitionCount(4);
            group.recordOffsetSequenceNumber(5);
        }
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
        committedOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        group.recordCommittedOffsets(committedOffsets);
        Map<TopicPartition, OffsetAndMetadata> consumedOffsets = new HashMap<>();
        consumedOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 10));
        group.recordConsumedOffsets(consumedOffsets);

        for (int i = 0; i != 20; ++i) {
            group1.recordRead(1);
            group1.recordSend(2);
            group1.recordPut(30);
            group1.recordPartitionCount(40);
            group1.recordOffsetSequenceNumber(50);
        }
        committedOffsets = new HashMap<>();
        committedOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET + 2));
        committedOffsets.put(TOPIC_PARTITION3, new OffsetAndMetadata(FIRST_OFFSET + 3));
        group1.recordCommittedOffsets(committedOffsets);
        consumedOffsets = new HashMap<>();
        consumedOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET + 20));
        consumedOffsets.put(TOPIC_PARTITION3, new OffsetAndMetadata(FIRST_OFFSET + 30));
        group1.recordConsumedOffsets(consumedOffsets);

        assertEquals(0.333, metrics.currentMetricValueAsDouble(group.metricGroup(), "sink-record-read-rate"), 0.001d);
        assertEquals(0.667, metrics.currentMetricValueAsDouble(group.metricGroup(), "sink-record-send-rate"), 0.001d);
        assertEquals(9, metrics.currentMetricValueAsDouble(group.metricGroup(), "sink-record-active-count"), 0.001d);
        assertEquals(4, metrics.currentMetricValueAsDouble(group.metricGroup(), "partition-count"), 0.001d);
        assertEquals(5, metrics.currentMetricValueAsDouble(group.metricGroup(), "offset-commit-seq-no"), 0.001d);
        assertEquals(3, metrics.currentMetricValueAsDouble(group.metricGroup(), "put-batch-max-time-ms"), 0.001d);

        // Close the group
        group.close();

        for (MetricName metricName : group.metricGroup().metrics().metrics().keySet()) {
            // Metrics for this group should no longer exist
            assertFalse(group.metricGroup().groupId().includes(metricName));
        }
        // Sensors for this group should no longer exist
        assertNull(group.metricGroup().metrics().getSensor("source-record-poll"));
        assertNull(group.metricGroup().metrics().getSensor("source-record-write"));
        assertNull(group.metricGroup().metrics().getSensor("poll-batch-time"));

        assertEquals(0.667, metrics.currentMetricValueAsDouble(group1.metricGroup(), "sink-record-read-rate"), 0.001d);
        assertEquals(1.333, metrics.currentMetricValueAsDouble(group1.metricGroup(), "sink-record-send-rate"), 0.001d);
        assertEquals(45, metrics.currentMetricValueAsDouble(group1.metricGroup(), "sink-record-active-count"), 0.001d);
        assertEquals(40, metrics.currentMetricValueAsDouble(group1.metricGroup(), "partition-count"), 0.001d);
        assertEquals(50, metrics.currentMetricValueAsDouble(group1.metricGroup(), "offset-commit-seq-no"), 0.001d);
        assertEquals(30, metrics.currentMetricValueAsDouble(group1.metricGroup(), "put-batch-max-time-ms"), 0.001d);
    }

    @Test
    public void testHeaders() {
        createTask(initialState);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        Headers headers = new RecordHeaders();
        headers.add("header_key", "header_value".getBytes());

        expectPollInitialAssignment()
                .thenAnswer(expectConsumerPoll(1, headers));

        expectConversionAndTransformation(null, headers);

        workerTask.iteration(); // iter 1 -- initial assignment
        workerTask.iteration(); // iter 2 -- deliver 1 record

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<SinkRecord>> recordCapture = ArgumentCaptor.forClass(Collection.class);
        verify(sinkTask, times(2)).put(recordCapture.capture());

        assertEquals(1, recordCapture.getValue().size());
        SinkRecord record = recordCapture.getValue().iterator().next();

        assertEquals("header_value", record.headers().lastWithName("header_key").value());
    }

    @Test
    public void testHeadersWithCustomConverter() {
        StringConverter stringConverter = new StringConverter();
        SampleConverterWithHeaders testConverter = new SampleConverterWithHeaders();

        createTask(initialState, stringConverter, testConverter, stringConverter);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        String keyA = "a";
        String valueA = "Árvíztűrő tükörfúrógép";
        Headers headersA = new RecordHeaders();
        String encodingA = "latin2";
        headersA.add("encoding", encodingA.getBytes());

        String keyB = "b";
        String valueB = "Тестовое сообщение";
        Headers headersB = new RecordHeaders();
        String encodingB = "koi8_r";
        headersB.add("encoding", encodingB.getBytes());

        expectPollInitialAssignment()
                .thenAnswer((Answer<ConsumerRecords<byte[], byte[]>>) invocation -> {
                    List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                            new ConsumerRecord<>(TOPIC, PARTITION, FIRST_OFFSET + recordsReturnedTp1 + 1, RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE,
                                    0, 0, keyA.getBytes(), valueA.getBytes(encodingA), headersA, Optional.empty()),
                            new ConsumerRecord<>(TOPIC, PARTITION, FIRST_OFFSET + recordsReturnedTp1 + 2, RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE,
                                    0, 0, keyB.getBytes(), valueB.getBytes(encodingB), headersB, Optional.empty())
                    );
                    return new ConsumerRecords<>(Collections.singletonMap(new TopicPartition(TOPIC, PARTITION), records));
                });

        expectTransformation(null);

        workerTask.iteration(); // iter 1 -- initial assignment
        workerTask.iteration(); // iter 2 -- deliver records

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<SinkRecord>> records = ArgumentCaptor.forClass(Collection.class);
        verify(sinkTask, times(2)).put(records.capture());

        Iterator<SinkRecord> iterator = records.getValue().iterator();

        SinkRecord recordA = iterator.next();
        assertEquals(keyA, recordA.key());
        assertEquals(valueA, recordA.value());

        SinkRecord recordB = iterator.next();
        assertEquals(keyB, recordB.key());
        assertEquals(valueB, recordB.value());
    }

    @Test
    public void testOriginalTopicWithTopicMutatingTransformations() {
        createTask(initialState);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        expectPollInitialAssignment()
                .thenAnswer(expectConsumerPoll(1));

        expectConversionAndTransformation("newtopic_", new RecordHeaders());

        workerTask.iteration(); // initial assignment
        workerTask.iteration(); // first record delivered

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<SinkRecord>> recordCapture = ArgumentCaptor.forClass(Collection.class);
        verify(sinkTask, times(2)).put(recordCapture.capture());

        assertEquals(1, recordCapture.getValue().size());
        SinkRecord record = recordCapture.getValue().iterator().next();
        assertEquals(TOPIC, record.originalTopic());
        assertEquals("newtopic_" + TOPIC, record.topic());
    }

    @Test
    public void testPartitionCountInCaseOfPartitionRevocation() {
        MockConsumer<byte[], byte[]> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        // Setting up Worker Sink Task to check metrics
        workerTask = new WorkerSinkTask(
                taskId, sinkTask, statusListener, TargetState.PAUSED, workerConfig, ClusterConfigState.EMPTY, metrics,
                keyConverter, valueConverter, errorHandlingMetrics, headerConverter,
                transformationChain, mockConsumer, pluginLoader, time,
                RetryWithToleranceOperatorTest.noopOperator(), null, statusBackingStore, Collections::emptyList);
        mockConsumer.updateBeginningOffsets(
                new HashMap<TopicPartition, Long>() {{
                    put(TOPIC_PARTITION, 0L);
                    put(TOPIC_PARTITION2, 0L);
                }}
        );
        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        // Initial Re-balance to assign INITIAL_ASSIGNMENT which is "TOPIC_PARTITION" and "TOPIC_PARTITION2"
        mockConsumer.rebalance(INITIAL_ASSIGNMENT);
        assertSinkMetricValue("partition-count", 2);
        // Revoked "TOPIC_PARTITION" and second re-balance with "TOPIC_PARTITION2"
        mockConsumer.rebalance(Collections.singleton(TOPIC_PARTITION2));
        assertSinkMetricValue("partition-count", 1);
        // Closing the Worker Sink Task which will update the partition count as 0.
        workerTask.close();
        assertSinkMetricValue("partition-count", 0);
    }

    private void expectRebalanceRevocationError(RuntimeException e) {
        when(sinkTask.preCommit(anyMap())).thenReturn(Collections.emptyMap());
        doThrow(e).when(sinkTask).close(INITIAL_ASSIGNMENT);
    }

    private void expectRebalanceAssignmentError(RuntimeException e) {
        when(sinkTask.preCommit(anyMap())).thenReturn(Collections.emptyMap());
        when(consumer.position(TOPIC_PARTITION)).thenReturn(FIRST_OFFSET);
        when(consumer.position(TOPIC_PARTITION2)).thenReturn(FIRST_OFFSET);

        doThrow(e).when(sinkTask).open(INITIAL_ASSIGNMENT);
    }

    private void verifyInitializeTask() {
        verify(consumer).subscribe(eq(asList(TOPIC)), rebalanceListener.capture());
        verify(sinkTask).initialize(sinkTaskContext.capture());
        verify(sinkTask).start(TASK_PROPS);
    }

    private OngoingStubbing<ConsumerRecords<byte[], byte[]>> expectPollInitialAssignment() {
        when(consumer.assignment()).thenReturn(INITIAL_ASSIGNMENT);
        INITIAL_ASSIGNMENT.forEach(tp -> when(consumer.position(tp)).thenReturn(FIRST_OFFSET));

        return when(consumer.poll(any(Duration.class))).thenAnswer(
                invocation -> {
                    rebalanceListener.getValue().onPartitionsAssigned(INITIAL_ASSIGNMENT);
                    return ConsumerRecords.empty();
                }
        );
    }

    private void verifyPollInitialAssignment() {
        verify(sinkTask).open(INITIAL_ASSIGNMENT);
        verify(consumer, atLeastOnce()).assignment();
        verify(sinkTask).put(Collections.emptyList());
    }

    private Answer<ConsumerRecords<byte[], byte[]>> expectConsumerPoll(final int numMessages) {
        return expectConsumerPoll(numMessages, RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, new RecordHeaders());
    }

    private Answer<ConsumerRecords<byte[], byte[]>> expectConsumerPoll(final int numMessages,  Headers headers) {
        return expectConsumerPoll(numMessages, RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, headers);
    }

    private Answer<ConsumerRecords<byte[], byte[]>> expectConsumerPoll(final int numMessages, final long timestamp, final TimestampType timestampType, Headers headers) {
        return invocation -> {
            List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
            for (int i = 0; i < numMessages; i++)
                records.add(new ConsumerRecord<>(TOPIC, PARTITION, FIRST_OFFSET + recordsReturnedTp1 + i, timestamp, timestampType,
                        0, 0, RAW_KEY, RAW_VALUE, headers, Optional.empty()));
            recordsReturnedTp1 += numMessages;
            return new ConsumerRecords<>(
                    numMessages > 0 ?
                            Collections.singletonMap(new TopicPartition(TOPIC, PARTITION), records)
                            : Collections.emptyMap()
            );
        };
    }

    private void expectConversionAndTransformation(final String topicPrefix, final Headers headers) {
        when(keyConverter.toConnectData(TOPIC, headers, RAW_KEY)).thenReturn(new SchemaAndValue(KEY_SCHEMA, KEY));
        when(valueConverter.toConnectData(TOPIC, headers, RAW_VALUE)).thenReturn(new SchemaAndValue(VALUE_SCHEMA, VALUE));

        for (Header header : headers) {
            when(headerConverter.toConnectHeader(TOPIC, header.key(), header.value())).thenReturn(new SchemaAndValue(VALUE_SCHEMA, new String(header.value())));
        }

        expectTransformation(topicPrefix);
    }

    @SuppressWarnings("unchecked")
    private void expectTransformation(final String topicPrefix) {
        when(transformationChain.apply(any(ProcessingContext.class), any(SinkRecord.class))).thenAnswer((Answer<SinkRecord>)
                invocation -> {
                    SinkRecord origRecord = invocation.getArgument(1);
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
                    ) : origRecord;
                });
    }

    private void expectTaskGetTopic() {
        when(statusBackingStore.getTopic(anyString(), anyString())).thenAnswer((Answer<TopicStatus>) invocation -> {
            String connector = invocation.getArgument(0, String.class);
            String topic = invocation.getArgument(1, String.class);
            return new TopicStatus(topic, new ConnectorTaskId(connector, 0), Time.SYSTEM.milliseconds());
        });
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
}
