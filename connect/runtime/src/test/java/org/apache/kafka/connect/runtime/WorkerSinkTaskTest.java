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
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
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
import org.apache.kafka.connect.errors.ConnectException;
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
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
        TASK_PROPS.put(TaskConfig.TASK_CLASS_CONFIG, SinkTask.class.getName());
    }

    private static final TaskConfig TASK_CONFIG = new TaskConfig(TASK_PROPS);

    private final ConnectorTaskId taskId = new ConnectorTaskId("job", 0);
    private final ConnectorTaskId taskId1 = new ConnectorTaskId("job", 1);
    private final TargetState initialState = TargetState.STARTED;
    private MockTime time;
    private WorkerSinkTask workerTask;
    @Mock
    private SinkTask sinkTask;
    private final ArgumentCaptor<WorkerSinkTaskContext> sinkTaskContext = ArgumentCaptor.forClass(WorkerSinkTaskContext.class);
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
    private final ArgumentCaptor<ConsumerRebalanceListener> rebalanceListener = ArgumentCaptor.forClass(ConsumerRebalanceListener.class);

    private long recordsReturnedTp1;
    private long recordsReturnedTp3;

    @BeforeEach
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

    @AfterEach
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
        INITIAL_ASSIGNMENT.forEach(tp -> verify(consumer).resume(singleton(tp)));
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

        doNothing()
                .doThrow(new RetriableException("retry"))
                .doNothing()
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
        INITIAL_ASSIGNMENT.forEach(tp -> verify(consumer).resume(Collections.singleton(tp)));

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
    @SuppressWarnings("unchecked")
    public void testPollRedeliveryWithConsumerRebalance() {
        createTask(initialState);
        expectTaskGetTopic();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        Set<TopicPartition> newAssignment = new HashSet<>(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2, TOPIC_PARTITION3));

        when(consumer.assignment())
                .thenReturn(INITIAL_ASSIGNMENT, INITIAL_ASSIGNMENT, INITIAL_ASSIGNMENT)
                .thenReturn(newAssignment, newAssignment, newAssignment)
                .thenReturn(Collections.singleton(TOPIC_PARTITION3),
                        Collections.singleton(TOPIC_PARTITION3),
                        Collections.singleton(TOPIC_PARTITION3));

        INITIAL_ASSIGNMENT.forEach(tp -> when(consumer.position(tp)).thenReturn(FIRST_OFFSET));
        when(consumer.position(TOPIC_PARTITION3)).thenReturn(FIRST_OFFSET);

        when(consumer.poll(any(Duration.class)))
                .thenAnswer((Answer<ConsumerRecords<byte[], byte[]>>) invocation -> {
                    rebalanceListener.getValue().onPartitionsAssigned(INITIAL_ASSIGNMENT);
                    return ConsumerRecords.empty();
                })
                .thenAnswer(expectConsumerPoll(1))
                // Empty consumer poll (all partitions are paused) with rebalance; one new partition is assigned
                .thenAnswer(invocation -> {
                    rebalanceListener.getValue().onPartitionsRevoked(Collections.emptySet());
                    rebalanceListener.getValue().onPartitionsAssigned(Collections.singleton(TOPIC_PARTITION3));
                    return ConsumerRecords.empty();
                })
                .thenAnswer(expectConsumerPoll(0))
                // Non-empty consumer poll; all initially-assigned partitions are revoked in rebalance, and new partitions are allowed to resume
                .thenAnswer(invocation -> {
                    ConsumerRecord<byte[], byte[]> newRecord = new ConsumerRecord<>(TOPIC, PARTITION3, FIRST_OFFSET, RAW_KEY, RAW_VALUE);

                    rebalanceListener.getValue().onPartitionsRevoked(INITIAL_ASSIGNMENT);
                    rebalanceListener.getValue().onPartitionsAssigned(Collections.emptyList());
                    return new ConsumerRecords<>(Map.of(TOPIC_PARTITION3, List.of(newRecord)),
                        Map.of(TOPIC_PARTITION3, new OffsetAndMetadata(FIRST_OFFSET + 1, Optional.empty(), "")));
                });
        expectConversionAndTransformation(null, new RecordHeaders());

        doNothing()
                // If a retriable exception is thrown, we should redeliver the same batch, pausing the consumer in the meantime
                .doThrow(new RetriableException("retry"))
                .doThrow(new RetriableException("retry"))
                .doThrow(new RetriableException("retry"))
                .doNothing()
                .when(sinkTask).put(any(Collection.class));

        workerTask.iteration();

        // Pause
        workerTask.iteration();
        verify(consumer).pause(INITIAL_ASSIGNMENT);

        workerTask.iteration();
        verify(sinkTask).open(Collections.singleton(TOPIC_PARTITION3));
        // All partitions are re-paused in order to pause any newly-assigned partitions so that redelivery efforts can continue
        verify(consumer).pause(newAssignment);

        workerTask.iteration();

        final Map<TopicPartition, OffsetAndMetadata> offsets = INITIAL_ASSIGNMENT.stream()
                .collect(Collectors.toMap(Function.identity(), tp -> new OffsetAndMetadata(FIRST_OFFSET)));
        when(sinkTask.preCommit(offsets)).thenReturn(offsets);
        newAssignment = Collections.singleton(TOPIC_PARTITION3);

        workerTask.iteration();
        verify(sinkTask).close(INITIAL_ASSIGNMENT);

        // All partitions are resumed, as all previously paused-for-redelivery partitions were revoked
        newAssignment.forEach(tp -> verify(consumer).resume(Collections.singleton(tp)));
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

        RuntimeException thrownException = assertThrows(RuntimeException.class, () -> workerTask.iteration());
        assertEquals(exception, thrownException);
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

        RuntimeException thrownException = assertThrows(RuntimeException.class, () -> workerTask.iteration());
        assertEquals(exception, thrownException);
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
            RuntimeException thrownException = assertThrows(RuntimeException.class, () -> workerTask.iteration());
            assertEquals(exception, thrownException);
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

    @Test
    @SuppressWarnings("unchecked")
    public void testPreCommitFailureAfterPartialRevocationAndAssignment() {
        createTask(initialState);
        expectTaskGetTopic();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        when(consumer.assignment())
                .thenReturn(INITIAL_ASSIGNMENT, INITIAL_ASSIGNMENT)
                .thenReturn(new HashSet<>(Collections.singletonList(TOPIC_PARTITION2)))
                .thenReturn(new HashSet<>(Collections.singletonList(TOPIC_PARTITION2)))
                .thenReturn(new HashSet<>(Collections.singletonList(TOPIC_PARTITION2)))
                .thenReturn(new HashSet<>(Arrays.asList(TOPIC_PARTITION2, TOPIC_PARTITION3)))
                .thenReturn(new HashSet<>(Arrays.asList(TOPIC_PARTITION2, TOPIC_PARTITION3)))
                .thenReturn(new HashSet<>(Arrays.asList(TOPIC_PARTITION2, TOPIC_PARTITION3)));

        INITIAL_ASSIGNMENT.forEach(tp -> when(consumer.position(tp)).thenReturn(FIRST_OFFSET));
        when(consumer.position(TOPIC_PARTITION3)).thenReturn(FIRST_OFFSET);

        // First poll; assignment is [TP1, TP2]
        when(consumer.poll(any(Duration.class)))
                .thenAnswer((Answer<ConsumerRecords<byte[], byte[]>>) invocation -> {
                    rebalanceListener.getValue().onPartitionsAssigned(INITIAL_ASSIGNMENT);
                    return ConsumerRecords.empty();
                })
                // Second poll; a single record is delivered from TP1
                .thenAnswer(expectConsumerPoll(1))
                // Third poll; assignment changes to [TP2]
                .thenAnswer(invocation -> {
                    rebalanceListener.getValue().onPartitionsRevoked(Collections.singleton(TOPIC_PARTITION));
                    rebalanceListener.getValue().onPartitionsAssigned(Collections.emptySet());
                    return ConsumerRecords.empty();
                })
                // Fourth poll; assignment changes to [TP2, TP3]
                .thenAnswer(invocation -> {
                    rebalanceListener.getValue().onPartitionsRevoked(Collections.emptySet());
                    rebalanceListener.getValue().onPartitionsAssigned(Collections.singleton(TOPIC_PARTITION3));
                    return ConsumerRecords.empty();
                })
                // Fifth poll; an offset commit takes place
                .thenAnswer(expectConsumerPoll(0));

        expectConversionAndTransformation(null, new RecordHeaders());

        // First iteration--first call to poll, first consumer assignment
        workerTask.iteration();
        // Second iteration--second call to poll, delivery of one record
        workerTask.iteration();
        // Third iteration--third call to poll, partial consumer revocation
        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        when(sinkTask.preCommit(offsets)).thenReturn(offsets);
        doNothing().when(consumer).commitSync(offsets);

        workerTask.iteration();
        verify(sinkTask).close(Collections.singleton(TOPIC_PARTITION));
        verify(sinkTask, times(2)).put(Collections.emptyList());

        // Fourth iteration--fourth call to poll, partial consumer assignment
        workerTask.iteration();

        verify(sinkTask).open(Collections.singleton(TOPIC_PARTITION3));

        final Map<TopicPartition, OffsetAndMetadata> workerCurrentOffsets = new HashMap<>();
        workerCurrentOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));
        workerCurrentOffsets.put(TOPIC_PARTITION3, new OffsetAndMetadata(FIRST_OFFSET));
        when(sinkTask.preCommit(workerCurrentOffsets)).thenThrow(new ConnectException("Failed to flush"));

        // Fifth iteration--task-requested offset commit with failure in SinkTask::preCommit
        sinkTaskContext.getValue().requestCommit();
        workerTask.iteration();

        verify(consumer).seek(TOPIC_PARTITION2, FIRST_OFFSET);
        verify(consumer).seek(TOPIC_PARTITION3, FIRST_OFFSET);
    }

    @Test
    public void testWakeupInCommitSyncCausesRetry() {
        createTask(initialState);

        workerTask.initialize(TASK_CONFIG);
        time.sleep(30000L);
        workerTask.initializeAndStart();
        time.sleep(30000L);
        verifyInitializeTask();

        expectTaskGetTopic();
        expectPollInitialAssignment()
                .thenAnswer(expectConsumerPoll(1))
                .thenAnswer(invocation -> {
                    rebalanceListener.getValue().onPartitionsRevoked(INITIAL_ASSIGNMENT);
                    rebalanceListener.getValue().onPartitionsAssigned(INITIAL_ASSIGNMENT);
                    return ConsumerRecords.empty();
                });
        expectConversionAndTransformation(null, new RecordHeaders());

        workerTask.iteration(); // poll for initial assignment
        time.sleep(30000L);

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        offsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));
        when(sinkTask.preCommit(offsets)).thenReturn(offsets);

        // first one raises wakeup
        doThrow(new WakeupException())
                // and succeed the second time
                .doNothing()
                .when(consumer).commitSync(offsets);

        workerTask.iteration(); // first record delivered

        workerTask.iteration(); // now rebalance with the wakeup triggered
        time.sleep(30000L);

        verify(sinkTask).close(INITIAL_ASSIGNMENT);
        verify(sinkTask, times(2)).open(INITIAL_ASSIGNMENT);

        INITIAL_ASSIGNMENT.forEach(tp -> verify(consumer).resume(Collections.singleton(tp)));

        verify(statusListener).onResume(taskId);

        assertSinkMetricValue("partition-count", 2);
        assertSinkMetricValue("sink-record-read-total", 1.0);
        assertSinkMetricValue("sink-record-send-total", 1.0);
        assertSinkMetricValue("sink-record-active-count", 0.0);
        assertSinkMetricValue("sink-record-active-count-max", 1.0);
        assertSinkMetricValue("sink-record-active-count-avg", 0.33333);
        assertSinkMetricValue("offset-commit-seq-no", 1.0);
        assertSinkMetricValue("offset-commit-completion-total", 1.0);
        assertSinkMetricValue("offset-commit-skip-total", 0.0);
        assertTaskMetricValue("status", "running");
        assertTaskMetricValue("running-ratio", 1.0);
        assertTaskMetricValue("pause-ratio", 0.0);
        assertTaskMetricValue("batch-size-max", 1.0);
        assertTaskMetricValue("batch-size-avg", 1.0);
        assertTaskMetricValue("offset-commit-max-time-ms", 0.0);
        assertTaskMetricValue("offset-commit-avg-time-ms", 0.0);
        assertTaskMetricValue("offset-commit-failure-percentage", 0.0);
        assertTaskMetricValue("offset-commit-success-percentage", 1.0);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testWakeupNotThrownDuringShutdown() {
        createTask(initialState);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        expectTaskGetTopic();
        expectPollInitialAssignment()
                .thenAnswer(expectConsumerPoll(1))
                .thenAnswer(invocation -> {
                    // stop the task during its second iteration
                    workerTask.stop();
                    return new ConsumerRecords<>(Map.of(), Map.of());
                });
        expectConversionAndTransformation(null, new RecordHeaders());

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        offsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));
        when(sinkTask.preCommit(offsets)).thenReturn(offsets);

        // fail the first time
        doThrow(new WakeupException())
                // and succeed the second time
                .doNothing()
                .when(consumer).commitSync(offsets);

        workerTask.execute();

        assertEquals(0, workerTask.commitFailures());
        verify(consumer).wakeup();
        verify(sinkTask).close(any(Collection.class));
    }

    @Test
    public void testRequestCommit() {
        createTask(initialState);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        expectTaskGetTopic();
        expectPollInitialAssignment()
                .thenAnswer(expectConsumerPoll(1))
                .thenAnswer(expectConsumerPoll(0));
        expectConversionAndTransformation(null, new RecordHeaders());

        // Initial assignment
        time.sleep(30000L);
        workerTask.iteration();
        assertSinkMetricValue("partition-count", 2);

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        offsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));
        when(sinkTask.preCommit(offsets)).thenReturn(offsets);

        // First record delivered
        workerTask.iteration();
        assertSinkMetricValue("partition-count", 2);
        assertSinkMetricValue("sink-record-read-total", 1.0);
        assertSinkMetricValue("sink-record-send-total", 1.0);
        assertSinkMetricValue("sink-record-active-count", 1.0);
        assertSinkMetricValue("sink-record-active-count-max", 1.0);
        assertSinkMetricValue("sink-record-active-count-avg", 0.333333);
        assertSinkMetricValue("offset-commit-seq-no", 0.0);
        assertSinkMetricValue("offset-commit-completion-total", 0.0);
        assertSinkMetricValue("offset-commit-skip-total", 0.0);
        assertTaskMetricValue("status", "running");
        assertTaskMetricValue("running-ratio", 1.0);
        assertTaskMetricValue("pause-ratio", 0.0);
        assertTaskMetricValue("batch-size-max", 1.0);
        assertTaskMetricValue("batch-size-avg", 0.5);
        assertTaskMetricValue("offset-commit-failure-percentage", 0.0);
        assertTaskMetricValue("offset-commit-success-percentage", 0.0);

        // Grab the commit time prior to requesting a commit.
        // This time should advance slightly after committing.
        // KAFKA-8229
        final long previousCommitValue = workerTask.getNextCommit();
        sinkTaskContext.getValue().requestCommit();
        assertTrue(sinkTaskContext.getValue().isCommitRequested());
        assertNotEquals(offsets, workerTask.lastCommittedOffsets());

        ArgumentCaptor<OffsetCommitCallback> callback = ArgumentCaptor.forClass(OffsetCommitCallback.class);
        time.sleep(10000L);
        workerTask.iteration(); // triggers the commit
        verify(consumer).commitAsync(eq(offsets), callback.capture());
        callback.getValue().onComplete(offsets, null);
        time.sleep(10000L);

        assertFalse(sinkTaskContext.getValue().isCommitRequested()); // should have been cleared
        assertEquals(offsets, workerTask.lastCommittedOffsets());
        assertEquals(0, workerTask.commitFailures());

        // Assert the next commit time advances slightly, the amount it advances
        // is the normal commit time less the two sleeps since it started each
        // of those sleeps were 10 seconds.
        // KAFKA-8229
        assertEquals(previousCommitValue +
                        (WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT - 10000L * 2),
                workerTask.getNextCommit(),
                "Should have only advanced by 40 seconds");

        assertSinkMetricValue("partition-count", 2);
        assertSinkMetricValue("sink-record-read-total", 1.0);
        assertSinkMetricValue("sink-record-send-total", 1.0);
        assertSinkMetricValue("sink-record-active-count", 0.0);
        assertSinkMetricValue("sink-record-active-count-max", 1.0);
        assertSinkMetricValue("sink-record-active-count-avg", 0.2);
        assertSinkMetricValue("offset-commit-seq-no", 1.0);
        assertSinkMetricValue("offset-commit-completion-total", 1.0);
        assertSinkMetricValue("offset-commit-skip-total", 0.0);
        assertTaskMetricValue("status", "running");
        assertTaskMetricValue("running-ratio", 1.0);
        assertTaskMetricValue("pause-ratio", 0.0);
        assertTaskMetricValue("batch-size-max", 1.0);
        assertTaskMetricValue("batch-size-avg", 0.33333);
        assertTaskMetricValue("offset-commit-max-time-ms", 0.0);
        assertTaskMetricValue("offset-commit-avg-time-ms", 0.0);
        assertTaskMetricValue("offset-commit-failure-percentage", 0.0);
        assertTaskMetricValue("offset-commit-success-percentage", 1.0);
    }

    @Test
    public void testPreCommit() {
        createTask(initialState);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        expectTaskGetTopic();
        expectPollInitialAssignment()
                .thenAnswer(expectConsumerPoll(2))
                .thenAnswer(expectConsumerPoll(0));
        expectConversionAndTransformation(null, new RecordHeaders());

        workerTask.iteration(); // iter 1 -- initial assignment

        final Map<TopicPartition, OffsetAndMetadata> workerStartingOffsets = new HashMap<>();
        workerStartingOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET));
        workerStartingOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));

        assertEquals(workerStartingOffsets, workerTask.currentOffsets());

        final Map<TopicPartition, OffsetAndMetadata> workerCurrentOffsets = new HashMap<>();
        workerCurrentOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 2));
        workerCurrentOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));

        final Map<TopicPartition, OffsetAndMetadata> taskOffsets = new HashMap<>();
        taskOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1)); // act like FIRST_OFFSET+2 has not yet been flushed by the task
        taskOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET + 1)); // should be ignored because > current offset
        taskOffsets.put(new TopicPartition(TOPIC, 3), new OffsetAndMetadata(FIRST_OFFSET)); // should be ignored because this partition is not assigned

        when(sinkTask.preCommit(workerCurrentOffsets)).thenReturn(taskOffsets);

        workerTask.iteration(); // iter 2 -- deliver 2 records

        final Map<TopicPartition, OffsetAndMetadata> committableOffsets = new HashMap<>();
        committableOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        committableOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));

        assertEquals(workerCurrentOffsets, workerTask.currentOffsets());
        assertEquals(workerStartingOffsets, workerTask.lastCommittedOffsets());

        sinkTaskContext.getValue().requestCommit();
        workerTask.iteration(); // iter 3 -- commit

        // Expect extra invalid topic partition to be filtered, which causes the consumer assignment to be logged
        ArgumentCaptor<OffsetCommitCallback> callback = ArgumentCaptor.forClass(OffsetCommitCallback.class);
        verify(consumer).commitAsync(eq(committableOffsets), callback.capture());
        callback.getValue().onComplete(committableOffsets, null);

        assertEquals(committableOffsets, workerTask.lastCommittedOffsets());
    }

    @Test
    public void testPreCommitFailure() {
        createTask(initialState);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        expectTaskGetTopic();
        expectPollInitialAssignment()
                // Put one message through the task to get some offsets to commit
                .thenAnswer(expectConsumerPoll(2))
                .thenAnswer(expectConsumerPoll(0));

        expectConversionAndTransformation(null, new RecordHeaders());

        workerTask.iteration(); // iter 1 -- initial assignment

        workerTask.iteration(); // iter 2 -- deliver 2 records

        // iter 3
        final Map<TopicPartition, OffsetAndMetadata> workerCurrentOffsets = new HashMap<>();
        workerCurrentOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 2));
        workerCurrentOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));
        when(sinkTask.preCommit(workerCurrentOffsets)).thenThrow(new ConnectException("Failed to flush"));

        sinkTaskContext.getValue().requestCommit();
        workerTask.iteration(); // iter 3 -- commit

        verify(consumer).seek(TOPIC_PARTITION, FIRST_OFFSET);
        verify(consumer).seek(TOPIC_PARTITION2, FIRST_OFFSET);
    }

    @Test
    public void testIgnoredCommit() {
        createTask(initialState);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        expectTaskGetTopic();
        // iter 1
        expectPollInitialAssignment()
                // iter 2
                .thenAnswer(expectConsumerPoll(1))
                .thenAnswer(expectConsumerPoll(0));

        expectConversionAndTransformation(null, new RecordHeaders());

        workerTask.iteration(); // iter 1 -- initial assignment

        final Map<TopicPartition, OffsetAndMetadata> workerStartingOffsets = new HashMap<>();
        workerStartingOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET));
        workerStartingOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));

        assertEquals(workerStartingOffsets, workerTask.currentOffsets());
        assertEquals(workerStartingOffsets, workerTask.lastCommittedOffsets());

        workerTask.iteration(); // iter 2 -- deliver 2 records

    // iter 3
        final Map<TopicPartition, OffsetAndMetadata> workerCurrentOffsets = new HashMap<>();
        workerCurrentOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        workerCurrentOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));

        when(sinkTask.preCommit(workerCurrentOffsets)).thenReturn(workerStartingOffsets);

        sinkTaskContext.getValue().requestCommit();
        // no actual consumer.commit() triggered
        workerTask.iteration(); // iter 3 -- commit
    }

    // Test that the commitTimeoutMs timestamp is correctly computed and checked in WorkerSinkTask.iteration()
    // when there is a long running commit in process. See KAFKA-4942 for more information.
    @Test
    public void testLongRunningCommitWithoutTimeout() {
        createTask(initialState);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        expectTaskGetTopic();
        expectPollInitialAssignment()
                .thenAnswer(expectConsumerPoll(1))
                // no actual consumer.commit() triggered
                .thenAnswer(expectConsumerPoll(0));
        expectConversionAndTransformation(null, new RecordHeaders());

        final Map<TopicPartition, OffsetAndMetadata> workerStartingOffsets = new HashMap<>();
        workerStartingOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET));
        workerStartingOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));

        workerTask.iteration(); // iter 1 -- initial assignment
        assertEquals(workerStartingOffsets, workerTask.currentOffsets());
        assertEquals(workerStartingOffsets, workerTask.lastCommittedOffsets());

        time.sleep(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_DEFAULT);
        workerTask.iteration(); // iter 2 -- deliver 2 records

        final Map<TopicPartition, OffsetAndMetadata> workerCurrentOffsets = new HashMap<>();
        workerCurrentOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        workerCurrentOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));

        // iter 3 - note that we return the current offset to indicate they should be committed
        when(sinkTask.preCommit(workerCurrentOffsets)).thenReturn(workerCurrentOffsets);

        sinkTaskContext.getValue().requestCommit();
        workerTask.iteration(); // iter 3 -- commit in progress

        // Make sure the "committing" flag didn't immediately get flipped back to false due to an incorrect timeout
        assertTrue(workerTask.isCommitting(), "Expected worker to be in the process of committing offsets");

        // Delay the result of trying to commit offsets to Kafka via the consumer.commitAsync method.
        ArgumentCaptor<OffsetCommitCallback> offsetCommitCallbackArgumentCaptor =
            ArgumentCaptor.forClass(OffsetCommitCallback.class);
        verify(consumer).commitAsync(eq(workerCurrentOffsets), offsetCommitCallbackArgumentCaptor.capture());

        final OffsetCommitCallback callback = offsetCommitCallbackArgumentCaptor.getValue();
        callback.onComplete(workerCurrentOffsets, null);

        assertEquals(workerCurrentOffsets, workerTask.currentOffsets());
        assertEquals(workerCurrentOffsets, workerTask.lastCommittedOffsets());
        assertFalse(workerTask.isCommitting());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSinkTasksHandleCloseErrors() {
        createTask(initialState);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        expectTaskGetTopic();
        expectPollInitialAssignment()
                // Put one message through the task to get some offsets to commit
                .thenAnswer(expectConsumerPoll(1))
                .thenAnswer(expectConsumerPoll(1));

        expectConversionAndTransformation(null, new RecordHeaders());

        doNothing()
                .doAnswer(invocation -> {
                    workerTask.stop();
                    return null;
                })
                .when(sinkTask).put(anyList());

        Throwable closeException = new RuntimeException();
        when(sinkTask.preCommit(anyMap())).thenReturn(Collections.emptyMap());

        // Throw another exception while closing the task's assignment
        doThrow(closeException).when(sinkTask).close(any(Collection.class));

        RuntimeException thrownException = assertThrows(RuntimeException.class, () -> workerTask.execute());
        assertEquals(closeException, thrownException);

        verify(consumer).wakeup();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSuppressCloseErrors() {
        createTask(initialState);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        expectTaskGetTopic();
        expectPollInitialAssignment()
                // Put one message through the task to get some offsets to commit
                .thenAnswer(expectConsumerPoll(1))
                .thenAnswer(expectConsumerPoll(1));

        expectConversionAndTransformation(null, new RecordHeaders());

        Throwable putException = new RuntimeException();
        Throwable closeException = new RuntimeException();

        doNothing()
                // Throw an exception on the next put to trigger shutdown behavior
                // This exception is the true "cause" of the failure
                .doThrow(putException)
                .when(sinkTask).put(anyList());

        when(sinkTask.preCommit(anyMap())).thenReturn(Collections.emptyMap());

        // Throw another exception while closing the task's assignment
        doThrow(closeException).when(sinkTask).close(any(Collection.class));

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();

        RuntimeException thrownException = assertThrows(ConnectException.class, () -> workerTask.execute());
        assertEquals(putException, thrownException.getCause(), "Exception from put should be the cause");
        assertTrue(thrownException.getSuppressed().length > 0, "Exception from close should be suppressed");
        assertEquals(closeException, thrownException.getSuppressed()[0]);
    }

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

        doNothing()
                .doNothing()
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

    // Verify that when commitAsync is called but the supplied callback is not called by the consumer before a
    // rebalance occurs, the async callback does not reset the last committed offset from the rebalance.
    // See KAFKA-5731 for more information.
    @Test
    public void testCommitWithOutOfOrderCallback() {
        createTask(initialState);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        // iter 1
        Answer<ConsumerRecords<byte[], byte[]>> consumerPollRebalance = invocation -> {
            rebalanceListener.getValue().onPartitionsAssigned(INITIAL_ASSIGNMENT);
            return ConsumerRecords.empty();
        };

        // iter 2
        expectTaskGetTopic();
        expectConversionAndTransformation(null, new RecordHeaders());

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

        // iter 3 - note that we return the current offset to indicate they should be committed
        when(sinkTask.preCommit(workerCurrentOffsets)).thenReturn(workerCurrentOffsets);

        // We need to delay the result of trying to commit offsets to Kafka via the consumer.commitAsync
        // method. We do this so that we can test that the callback is not called until after the rebalance
        // changes the lastCommittedOffsets. To fake this for tests we have the commitAsync build a function
        // that will call the callback with the appropriate parameters, and we'll run that function later.
        final AtomicReference<Runnable> asyncCallbackRunner = new AtomicReference<>();
        final AtomicBoolean asyncCallbackRan = new AtomicBoolean();

        doAnswer(invocation -> {
            final Map<TopicPartition, OffsetAndMetadata> offsets = invocation.getArgument(0);
            final OffsetCommitCallback callback =  invocation.getArgument(1);
            asyncCallbackRunner.set(() -> {
                callback.onComplete(offsets, null);
                asyncCallbackRan.set(true);
            });

            return null;
        }).when(consumer).commitAsync(eq(workerCurrentOffsets), any(OffsetCommitCallback.class));

        // Expect the next poll to discover and perform the rebalance, THEN complete the previous callback handler,
        // and then return one record for TP1 and one for TP3.
        final AtomicBoolean rebalanced = new AtomicBoolean();
        Answer<ConsumerRecords<byte[], byte[]>> consumerPollRebalanced = invocation -> {
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
            final TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
            final OffsetAndMetadata nextOffsetAndMetadata = new OffsetAndMetadata(FIRST_OFFSET + recordsReturnedTp1 + 2, Optional.empty(), "");
            return new ConsumerRecords<>(Map.of(tp, records), Map.of(tp, nextOffsetAndMetadata));
        };

        // onPartitionsRevoked
        when(sinkTask.preCommit(workerCurrentOffsets)).thenReturn(workerCurrentOffsets);

        // onPartitionsAssigned - step 1
        final long offsetTp1 = rebalanceOffsets.get(TOPIC_PARTITION).offset();
        final long offsetTp2 = rebalanceOffsets.get(TOPIC_PARTITION2).offset();
        final long offsetTp3 = rebalanceOffsets.get(TOPIC_PARTITION3).offset();

        // iter 4 - note that we return the current offset to indicate they should be committed
        when(sinkTask.preCommit(postRebalanceCurrentOffsets)).thenReturn(postRebalanceCurrentOffsets);

        // Setup mocks
        when(consumer.assignment())
                .thenReturn(INITIAL_ASSIGNMENT)
                .thenReturn(INITIAL_ASSIGNMENT)
                .thenReturn(INITIAL_ASSIGNMENT)
                .thenReturn(INITIAL_ASSIGNMENT)
                .thenReturn(INITIAL_ASSIGNMENT)
                .thenReturn(new HashSet<>(rebalancedPartitions))
                .thenReturn(new HashSet<>(rebalancedPartitions))
                .thenReturn(new HashSet<>(rebalancedPartitions))
                .thenReturn(new HashSet<>(rebalancedPartitions))
                .thenReturn(new HashSet<>(rebalancedPartitions));

        when(consumer.position(TOPIC_PARTITION))
                .thenReturn(FIRST_OFFSET)
                .thenReturn(offsetTp1);

        when(consumer.position(TOPIC_PARTITION2))
                .thenReturn(FIRST_OFFSET)
                .thenReturn(offsetTp2);

        when(consumer.position(TOPIC_PARTITION3))
                .thenReturn(offsetTp3);

        when(consumer.poll(any(Duration.class)))
                .thenAnswer(consumerPollRebalance)
                .thenAnswer(expectConsumerPoll(1))
                .thenAnswer(consumerPollRebalanced)
                .thenAnswer(expectConsumerPoll(1));

        // Run the iterations
        workerTask.iteration(); // iter 1 -- initial assignment

        time.sleep(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_DEFAULT);
        workerTask.iteration(); // iter 2 -- deliver records

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
        assertEquals(postRebalanceCurrentOffsets, workerTask.currentOffsets());
        assertEquals(rebalanceOffsets, workerTask.lastCommittedOffsets());

        // onPartitionsRevoked
        verify(sinkTask).close(new ArrayList<>(workerCurrentOffsets.keySet()));
        verify(consumer).commitSync(anyMap());

        // onPartitionsAssigned - step 2
        verify(sinkTask).open(rebalancedPartitions);

        // onPartitionsAssigned - step 3 rewind
        verify(consumer).seek(TOPIC_PARTITION, offsetTp1);
        verify(consumer).seek(TOPIC_PARTITION2, offsetTp2);
        verify(consumer).seek(TOPIC_PARTITION3, offsetTp3);

        time.sleep(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_DEFAULT);
        sinkTaskContext.getValue().requestCommit();
        workerTask.iteration(); // iter 4 -- commit in progress

        final ArgumentCaptor<OffsetCommitCallback> callback = ArgumentCaptor.forClass(OffsetCommitCallback.class);
        verify(consumer).commitAsync(eq(postRebalanceCurrentOffsets), callback.capture());
        callback.getValue().onComplete(postRebalanceCurrentOffsets, null);

        // Check that the offsets were not reset by the out-of-order async commit callback
        assertEquals(postRebalanceCurrentOffsets, workerTask.currentOffsets());
        assertEquals(postRebalanceCurrentOffsets, workerTask.lastCommittedOffsets());

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
                    final OffsetAndMetadata nextOffsetAndMetadata = new OffsetAndMetadata(FIRST_OFFSET + recordsReturnedTp1 + 3, Optional.empty(), "");
                    final TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
                    return new ConsumerRecords<>(Map.of(tp, records), Map.of(tp, nextOffsetAndMetadata));
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
        MockConsumer<byte[], byte[]> mockConsumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name());
        // Setting up Worker Sink Task to check metrics
        workerTask = new WorkerSinkTask(
                taskId, sinkTask, statusListener, TargetState.PAUSED, workerConfig, ClusterConfigState.EMPTY, metrics,
                keyConverter, valueConverter, errorHandlingMetrics, headerConverter,
                transformationChain, mockConsumer, pluginLoader, time,
                RetryWithToleranceOperatorTest.noopOperator(), null, statusBackingStore, Collections::emptyList);
        mockConsumer.updateBeginningOffsets(
                new HashMap<>() {{
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
        verify(consumer).subscribe(eq(Collections.singletonList(TOPIC)), rebalanceListener.capture());
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
            long offset = 0;
            for (int i = 0; i < numMessages; i++) {
                offset = FIRST_OFFSET + recordsReturnedTp1 + i;
                records.add(new ConsumerRecord<>(TOPIC, PARTITION, offset, timestamp, timestampType,
                    0, 0, RAW_KEY, RAW_VALUE, headers, Optional.empty()));
            }
            recordsReturnedTp1 += numMessages;
            final TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
            if (numMessages > 0) {
                return new ConsumerRecords<>(Map.of(tp, records), Map.of(tp, new OffsetAndMetadata(offset + 1, Optional.empty(), "")));
            }
            return new ConsumerRecords<>(Map.of(), Map.of());
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
