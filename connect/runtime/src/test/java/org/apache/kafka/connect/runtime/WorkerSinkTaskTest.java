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

import java.util.Arrays;
import java.util.Iterator;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
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
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.runtime.WorkerSinkTask.SinkTaskMetricsGroup;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperatorTest;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.isolation.PluginClassLoader;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.easymock.Capture;
import org.easymock.CaptureType;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
    private ConnectorTaskId taskId1 = new ConnectorTaskId("job", 1);
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
    private TransformationChain<SinkRecord> transformationChain;
    @Mock
    private TaskStatus.Listener statusListener;
    @Mock
    private StatusBackingStore statusBackingStore;
    @Mock
    private KafkaConsumer<byte[], byte[]> consumer;
    @Mock
    private ErrorHandlingMetrics errorHandlingMetrics;
    private Capture<ConsumerRebalanceListener> rebalanceListener = EasyMock.newCapture();
    private Capture<Pattern> topicsRegex = EasyMock.newCapture();

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
        workerTask = new WorkerSinkTask(
            taskId, sinkTask, statusListener, initialState, workerConfig, ClusterConfigState.EMPTY, metrics,
            keyConverter, valueConverter, errorHandlingMetrics, headerConverter,
            transformationChain, consumer, pluginLoader, time,
            RetryWithToleranceOperatorTest.NOOP_OPERATOR, null, statusBackingStore);
    }

    @After
    public void tearDown() {
        if (metrics != null) metrics.stop();
    }

    @Test
    public void testStartPaused() throws Exception {
        createTask(TargetState.PAUSED);

        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();

        EasyMock.expect(consumer.assignment()).andReturn(INITIAL_ASSIGNMENT);
        consumer.pause(INITIAL_ASSIGNMENT);
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration();
        time.sleep(10000L);

        assertSinkMetricValue("partition-count", 2);
        assertTaskMetricValue("status", "paused");
        assertTaskMetricValue("running-ratio", 0.0);
        assertTaskMetricValue("pause-ratio", 1.0);
        assertTaskMetricValue("offset-commit-max-time-ms", Double.NaN);

        PowerMock.verifyAll();
    }

    @Test
    public void testPause() throws Exception {
        createTask(initialState);

        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();

        expectConsumerPoll(1);
        expectConversionAndTransformation(1);
        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall();

        // Pause
        statusListener.onPause(taskId);
        EasyMock.expectLastCall();
        expectConsumerWakeup();
        EasyMock.expect(consumer.assignment()).andReturn(INITIAL_ASSIGNMENT);
        consumer.pause(INITIAL_ASSIGNMENT);
        PowerMock.expectLastCall();

        // Offset commit as requested when pausing; No records returned by consumer.poll()
        sinkTask.preCommit(EasyMock.anyObject());
        EasyMock.expectLastCall().andStubReturn(Collections.emptyMap());
        expectConsumerPoll(0);
        sinkTask.put(Collections.emptyList());
        EasyMock.expectLastCall();

        // And unpause
        statusListener.onResume(taskId);
        EasyMock.expectLastCall();
        expectConsumerWakeup();
        EasyMock.expect(consumer.assignment()).andReturn(INITIAL_ASSIGNMENT).times(2);
        INITIAL_ASSIGNMENT.forEach(tp -> {
            consumer.resume(Collections.singleton(tp));
            PowerMock.expectLastCall();
        });

        expectConsumerPoll(1);
        expectConversionAndTransformation(1);
        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration(); // initial assignment
        workerTask.iteration(); // fetch some data
        workerTask.transitionTo(TargetState.PAUSED);
        time.sleep(10000L);

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

        workerTask.transitionTo(TargetState.STARTED);
        workerTask.iteration(); // wakeup
        workerTask.iteration(); // now unpaused
        //printMetrics();

        PowerMock.verifyAll();
    }

    @Test
    public void testShutdown() throws Exception {
        createTask(initialState);

        expectInitializeTask();
        expectTaskGetTopic(true);

        // first iteration
        expectPollInitialAssignment();

        // second iteration
        EasyMock.expect(sinkTask.preCommit(EasyMock.anyObject())).andReturn(Collections.emptyMap());
        expectConsumerPoll(1);
        expectConversionAndTransformation(1);
        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall();

        // WorkerSinkTask::stop
        consumer.wakeup();
        PowerMock.expectLastCall();
        sinkTask.stop();
        PowerMock.expectLastCall();

        EasyMock.expect(consumer.assignment()).andReturn(INITIAL_ASSIGNMENT);
        // WorkerSinkTask::close
        consumer.close();
        PowerMock.expectLastCall().andAnswer(() -> {
            rebalanceListener.getValue().onPartitionsRevoked(
                INITIAL_ASSIGNMENT
            );
            return null;
        });
        transformationChain.close();
        PowerMock.expectLastCall();

        headerConverter.close();
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration();
        sinkTaskContext.getValue().requestCommit(); // Force an offset commit
        workerTask.iteration();
        workerTask.stop();
        workerTask.close();

        PowerMock.verifyAll();
    }

    @Test
    public void testPollRedelivery() throws Exception {
        createTask(initialState);

        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();

        // If a retriable exception is thrown, we should redeliver the same batch, pausing the consumer in the meantime
        expectConsumerPoll(1);
        expectConversionAndTransformation(1);
        Capture<Collection<SinkRecord>> records = EasyMock.newCapture(CaptureType.ALL);
        sinkTask.put(EasyMock.capture(records));
        EasyMock.expectLastCall().andThrow(new RetriableException("retry"));
        // Pause
        EasyMock.expect(consumer.assignment()).andReturn(INITIAL_ASSIGNMENT);
        consumer.pause(INITIAL_ASSIGNMENT);
        PowerMock.expectLastCall();

        // Retry delivery should succeed
        expectConsumerPoll(0);
        sinkTask.put(EasyMock.capture(records));
        EasyMock.expectLastCall();
        // And unpause
        EasyMock.expect(consumer.assignment()).andReturn(INITIAL_ASSIGNMENT);
        INITIAL_ASSIGNMENT.forEach(tp -> {
            consumer.resume(singleton(tp));
            PowerMock.expectLastCall();
        });

        // Expect commit
        EasyMock.expect(consumer.assignment()).andReturn(INITIAL_ASSIGNMENT).times(2);
        final Map<TopicPartition, OffsetAndMetadata> workerCurrentOffsets = new HashMap<>();
        // Commit advance by one
        workerCurrentOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        // Nothing polled for this partition
        workerCurrentOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));
        EasyMock.expect(sinkTask.preCommit(workerCurrentOffsets)).andReturn(workerCurrentOffsets);
        final Capture<OffsetCommitCallback> callback = EasyMock.newCapture();
        consumer.commitAsync(EasyMock.eq(workerCurrentOffsets), EasyMock.capture(callback));
        EasyMock.expectLastCall().andAnswer(() -> {
            callback.getValue().onComplete(workerCurrentOffsets, null);
            return null;
        });
        expectConsumerPoll(0);
        sinkTask.put(EasyMock.eq(Collections.emptyList()));
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration();
        time.sleep(10000L);

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

        workerTask.iteration();
        workerTask.iteration();
        time.sleep(30000L);

        assertSinkMetricValue("sink-record-read-total", 1.0);
        assertSinkMetricValue("sink-record-send-total", 1.0);
        assertSinkMetricValue("sink-record-active-count", 1.0);
        assertSinkMetricValue("sink-record-active-count-max", 1.0);
        assertSinkMetricValue("sink-record-active-count-avg", 0.5);
        assertTaskMetricValue("status", "running");
        assertTaskMetricValue("running-ratio", 1.0);
        assertTaskMetricValue("batch-size-max", 1.0);
        assertTaskMetricValue("batch-size-avg", 0.5);
        
        sinkTaskContext.getValue().requestCommit();
        time.sleep(10000L);
        workerTask.iteration();
        assertSinkMetricValue("offset-commit-completion-total", 1.0);

        PowerMock.verifyAll();
    }

    @Test
    public void testPollRedeliveryWithConsumerRebalance() throws Exception {
        createTask(initialState);

        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();

        // If a retriable exception is thrown, we should redeliver the same batch, pausing the consumer in the meantime
        expectConsumerPoll(1);
        expectConversionAndTransformation(1);
        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall().andThrow(new RetriableException("retry"));
        // Pause
        EasyMock.expect(consumer.assignment()).andReturn(INITIAL_ASSIGNMENT);
        consumer.pause(INITIAL_ASSIGNMENT);
        PowerMock.expectLastCall();

        // Empty consumer poll (all partitions are paused) with rebalance; one new partition is assigned
        EasyMock.expect(consumer.poll(Duration.ofMillis(EasyMock.anyLong()))).andAnswer(
            () -> {
                rebalanceListener.getValue().onPartitionsRevoked(Collections.emptySet());
                rebalanceListener.getValue().onPartitionsAssigned(Collections.singleton(TOPIC_PARTITION3));
                return ConsumerRecords.empty();
            });
        Set<TopicPartition> newAssignment = new HashSet<>(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2, TOPIC_PARTITION3));
        EasyMock.expect(consumer.assignment()).andReturn(newAssignment).times(3);
        EasyMock.expect(consumer.position(TOPIC_PARTITION3)).andReturn(FIRST_OFFSET);
        sinkTask.open(Collections.singleton(TOPIC_PARTITION3));
        EasyMock.expectLastCall();
        // All partitions are re-paused in order to pause any newly-assigned partitions so that redelivery efforts can continue
        consumer.pause(newAssignment);
        EasyMock.expectLastCall();
        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall().andThrow(new RetriableException("retry"));

        // Next delivery attempt fails again
        expectConsumerPoll(0);
        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall().andThrow(new RetriableException("retry"));

        // Non-empty consumer poll; all initially-assigned partitions are revoked in rebalance, and new partitions are allowed to resume
        ConsumerRecord<byte[], byte[]> newRecord = new ConsumerRecord<>(TOPIC, PARTITION3, FIRST_OFFSET, RAW_KEY, RAW_VALUE);
        EasyMock.expect(consumer.poll(Duration.ofMillis(EasyMock.anyLong()))).andAnswer(
            () -> {
                rebalanceListener.getValue().onPartitionsRevoked(INITIAL_ASSIGNMENT);
                rebalanceListener.getValue().onPartitionsAssigned(Collections.emptyList());
                return new ConsumerRecords<>(Collections.singletonMap(TOPIC_PARTITION3, Collections.singletonList(newRecord)));
            });
        newAssignment = Collections.singleton(TOPIC_PARTITION3);
        EasyMock.expect(consumer.assignment()).andReturn(new HashSet<>(newAssignment)).times(3);
        final Map<TopicPartition, OffsetAndMetadata> offsets = INITIAL_ASSIGNMENT.stream()
                .collect(Collectors.toMap(Function.identity(), tp -> new OffsetAndMetadata(FIRST_OFFSET)));
        sinkTask.preCommit(offsets);
        EasyMock.expectLastCall().andReturn(offsets);
        sinkTask.close(INITIAL_ASSIGNMENT);
        EasyMock.expectLastCall();
        // All partitions are resumed, as all previously paused-for-redelivery partitions were revoked
        newAssignment.forEach(tp -> {
            consumer.resume(Collections.singleton(tp));
            EasyMock.expectLastCall();
        });
        expectConversionAndTransformation(1);
        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration();
        workerTask.iteration();
        workerTask.iteration();
        workerTask.iteration();
        workerTask.iteration();

        PowerMock.verifyAll();
    }

    @Test
    public void testErrorInRebalancePartitionLoss() throws Exception {
        RuntimeException exception = new RuntimeException("Revocation error");

        createTask(initialState);

        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();
        expectRebalanceLossError(exception);

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration();
        try {
            workerTask.iteration();
            fail("Poll should have raised the rebalance exception");
        } catch (RuntimeException e) {
            assertEquals(exception, e);
        }

        PowerMock.verifyAll();
    }

    @Test
    public void testErrorInRebalancePartitionRevocation() throws Exception {
        RuntimeException exception = new RuntimeException("Revocation error");

        createTask(initialState);

        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();
        expectRebalanceRevocationError(exception);

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration();
        try {
            workerTask.iteration();
            fail("Poll should have raised the rebalance exception");
        } catch (RuntimeException e) {
            assertEquals(exception, e);
        }

        PowerMock.verifyAll();
    }

    @Test
    public void testErrorInRebalancePartitionAssignment() throws Exception {
        RuntimeException exception = new RuntimeException("Assignment error");

        createTask(initialState);

        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();
        expectRebalanceAssignmentError(exception);

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration();
        try {
            workerTask.iteration();
            fail("Poll should have raised the rebalance exception");
        } catch (RuntimeException e) {
            assertEquals(exception, e);
        }

        PowerMock.verifyAll();
    }

    @Test
    public void testPartialRevocationAndAssignment() throws Exception {
        createTask(initialState);

        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();

        EasyMock.expect(consumer.poll(Duration.ofMillis(EasyMock.anyLong()))).andAnswer(
            () -> {
                rebalanceListener.getValue().onPartitionsRevoked(Collections.singleton(TOPIC_PARTITION));
                rebalanceListener.getValue().onPartitionsAssigned(Collections.emptySet());
                return ConsumerRecords.empty();
            });
        EasyMock.expect(consumer.assignment()).andReturn(Collections.singleton(TOPIC_PARTITION)).times(2);
        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET));
        sinkTask.preCommit(offsets);
        EasyMock.expectLastCall().andReturn(offsets);
        sinkTask.close(Collections.singleton(TOPIC_PARTITION));
        EasyMock.expectLastCall();
        sinkTask.put(Collections.emptyList());
        EasyMock.expectLastCall();

        EasyMock.expect(consumer.poll(Duration.ofMillis(EasyMock.anyLong()))).andAnswer(
            () -> {
                rebalanceListener.getValue().onPartitionsRevoked(Collections.emptySet());
                rebalanceListener.getValue().onPartitionsAssigned(Collections.singleton(TOPIC_PARTITION3));
                return ConsumerRecords.empty();
            });
        EasyMock.expect(consumer.assignment()).andReturn(new HashSet<>(Arrays.asList(TOPIC_PARTITION2, TOPIC_PARTITION3))).times(2);
        EasyMock.expect(consumer.position(TOPIC_PARTITION3)).andReturn(FIRST_OFFSET);
        sinkTask.open(Collections.singleton(TOPIC_PARTITION3));
        EasyMock.expectLastCall();
        sinkTask.put(Collections.emptyList());
        EasyMock.expectLastCall();

        EasyMock.expect(consumer.poll(Duration.ofMillis(EasyMock.anyLong()))).andAnswer(
            () -> {
                rebalanceListener.getValue().onPartitionsLost(Collections.singleton(TOPIC_PARTITION3));
                rebalanceListener.getValue().onPartitionsAssigned(Collections.singleton(TOPIC_PARTITION));
                return ConsumerRecords.empty();
            });
        EasyMock.expect(consumer.assignment()).andReturn(INITIAL_ASSIGNMENT).times(4);
        sinkTask.close(Collections.singleton(TOPIC_PARTITION3));
        EasyMock.expectLastCall();
        EasyMock.expect(consumer.position(TOPIC_PARTITION)).andReturn(FIRST_OFFSET);
        sinkTask.open(Collections.singleton(TOPIC_PARTITION));
        EasyMock.expectLastCall();
        sinkTask.put(Collections.emptyList());
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        // First iteration--first call to poll, first consumer assignment
        workerTask.iteration();
        // Second iteration--second call to poll, partial consumer revocation
        workerTask.iteration();
        // Third iteration--third call to poll, partial consumer assignment
        workerTask.iteration();
        // Fourth iteration--fourth call to poll, one partition lost; can't commit offsets for it, one new partition assigned
        workerTask.iteration();

        PowerMock.verifyAll();
    }

    @Test
    public void testPreCommitFailureAfterPartialRevocationAndAssignment() throws Exception {
        createTask(initialState);

        // First poll; assignment is [TP1, TP2]
        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();

        // Second poll; a single record is delivered from TP1
        expectConsumerPoll(1);
        expectConversionAndTransformation(1);
        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall();

        // Third poll; assignment changes to [TP2]
        EasyMock.expect(consumer.poll(Duration.ofMillis(EasyMock.anyLong()))).andAnswer(
                () -> {
                    rebalanceListener.getValue().onPartitionsRevoked(Collections.singleton(TOPIC_PARTITION));
                    rebalanceListener.getValue().onPartitionsAssigned(Collections.emptySet());
                    return ConsumerRecords.empty();
                });
        EasyMock.expect(consumer.assignment()).andReturn(Collections.singleton(TOPIC_PARTITION)).times(2);
        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        sinkTask.preCommit(offsets);
        EasyMock.expectLastCall().andReturn(offsets);
        consumer.commitSync(offsets);
        EasyMock.expectLastCall();
        sinkTask.close(Collections.singleton(TOPIC_PARTITION));
        EasyMock.expectLastCall();
        sinkTask.put(Collections.emptyList());
        EasyMock.expectLastCall();

        // Fourth poll; assignment changes to [TP2, TP3]
        EasyMock.expect(consumer.poll(Duration.ofMillis(EasyMock.anyLong()))).andAnswer(
                () -> {
                    rebalanceListener.getValue().onPartitionsRevoked(Collections.emptySet());
                    rebalanceListener.getValue().onPartitionsAssigned(Collections.singleton(TOPIC_PARTITION3));
                    return ConsumerRecords.empty();
                });
        EasyMock.expect(consumer.assignment()).andReturn(new HashSet<>(Arrays.asList(TOPIC_PARTITION2, TOPIC_PARTITION3))).times(2);
        EasyMock.expect(consumer.position(TOPIC_PARTITION3)).andReturn(FIRST_OFFSET);
        sinkTask.open(Collections.singleton(TOPIC_PARTITION3));
        EasyMock.expectLastCall();
        sinkTask.put(Collections.emptyList());
        EasyMock.expectLastCall();

        // Fifth poll; an offset commit takes place
        EasyMock.expect(consumer.assignment()).andReturn(new HashSet<>(Arrays.asList(TOPIC_PARTITION2, TOPIC_PARTITION3))).times(2);
        final Map<TopicPartition, OffsetAndMetadata> workerCurrentOffsets = new HashMap<>();
        workerCurrentOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));
        workerCurrentOffsets.put(TOPIC_PARTITION3, new OffsetAndMetadata(FIRST_OFFSET));
        sinkTask.preCommit(workerCurrentOffsets);
        EasyMock.expectLastCall().andThrow(new ConnectException("Failed to flush"));

        consumer.seek(TOPIC_PARTITION2, FIRST_OFFSET);
        EasyMock.expectLastCall();
        consumer.seek(TOPIC_PARTITION3, FIRST_OFFSET);
        EasyMock.expectLastCall();

        expectConsumerPoll(0);
        sinkTask.put(EasyMock.eq(Collections.emptyList()));
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        // First iteration--first call to poll, first consumer assignment
        workerTask.iteration();
        // Second iteration--second call to poll, delivery of one record
        workerTask.iteration();
        // Third iteration--third call to poll, partial consumer revocation
        workerTask.iteration();
        // Fourth iteration--fourth call to poll, partial consumer assignment
        workerTask.iteration();
        // Fifth iteration--task-requested offset commit with failure in SinkTask::preCommit
        sinkTaskContext.getValue().requestCommit();
        workerTask.iteration();

        PowerMock.verifyAll();
    }

    @Test
    public void testWakeupInCommitSyncCausesRetry() throws Exception {
        createTask(initialState);

        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();

        expectConsumerPoll(1);
        expectConversionAndTransformation(1);
        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall();

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        offsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));
        sinkTask.preCommit(offsets);
        EasyMock.expectLastCall().andReturn(offsets);

        // first one raises wakeup
        consumer.commitSync(EasyMock.<Map<TopicPartition, OffsetAndMetadata>>anyObject());
        EasyMock.expectLastCall().andThrow(new WakeupException());

        // we should retry and complete the commit
        consumer.commitSync(EasyMock.<Map<TopicPartition, OffsetAndMetadata>>anyObject());
        EasyMock.expectLastCall();

        sinkTask.close(INITIAL_ASSIGNMENT);
        EasyMock.expectLastCall();

        INITIAL_ASSIGNMENT.forEach(tp -> EasyMock.expect(consumer.position(tp)).andReturn(FIRST_OFFSET));

        sinkTask.open(INITIAL_ASSIGNMENT);
        EasyMock.expectLastCall();

        EasyMock.expect(consumer.assignment()).andReturn(INITIAL_ASSIGNMENT).times(5);
        EasyMock.expect(consumer.poll(Duration.ofMillis(EasyMock.anyLong()))).andAnswer(
            () -> {
                rebalanceListener.getValue().onPartitionsRevoked(INITIAL_ASSIGNMENT);
                rebalanceListener.getValue().onPartitionsAssigned(INITIAL_ASSIGNMENT);
                return ConsumerRecords.empty();
            });

        INITIAL_ASSIGNMENT.forEach(tp -> {
            consumer.resume(Collections.singleton(tp));
            EasyMock.expectLastCall();
        });

        statusListener.onResume(taskId);
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        time.sleep(30000L);
        workerTask.initializeAndStart();
        time.sleep(30000L);

        workerTask.iteration(); // poll for initial assignment
        time.sleep(30000L);
        workerTask.iteration(); // first record delivered
        workerTask.iteration(); // now rebalance with the wakeup triggered
        time.sleep(30000L);

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

        PowerMock.verifyAll();
    }

    @Test
    public void testWakeupNotThrownDuringShutdown() throws Exception {
        createTask(initialState);

        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();

        expectConsumerPoll(1);
        expectConversionAndTransformation(1);
        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall();

        EasyMock.expect(consumer.poll(Duration.ofMillis(EasyMock.anyLong()))).andAnswer(() -> {
            // stop the task during its second iteration
            workerTask.stop();
            return new ConsumerRecords<>(Collections.emptyMap());
        });
        consumer.wakeup();
        EasyMock.expectLastCall();

        sinkTask.put(EasyMock.eq(Collections.emptyList()));
        EasyMock.expectLastCall();

        EasyMock.expect(consumer.assignment()).andReturn(INITIAL_ASSIGNMENT).times(1);

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        offsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));
        sinkTask.preCommit(offsets);
        EasyMock.expectLastCall().andReturn(offsets);

        sinkTask.close(EasyMock.anyObject());
        PowerMock.expectLastCall();

        // fail the first time
        consumer.commitSync(EasyMock.eq(offsets));
        EasyMock.expectLastCall().andThrow(new WakeupException());

        // and succeed the second time
        consumer.commitSync(EasyMock.eq(offsets));
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.execute();

        assertEquals(0, workerTask.commitFailures());

        PowerMock.verifyAll();
    }

    @Test
    public void testRequestCommit() throws Exception {
        createTask(initialState);

        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();

        expectConsumerPoll(1);
        expectConversionAndTransformation(1);
        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall();

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        offsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));
        sinkTask.preCommit(offsets);
        EasyMock.expectLastCall().andReturn(offsets);

        EasyMock.expect(consumer.assignment()).andReturn(INITIAL_ASSIGNMENT).times(2);

        final Capture<OffsetCommitCallback> callback = EasyMock.newCapture();
        consumer.commitAsync(EasyMock.eq(offsets), EasyMock.capture(callback));
        EasyMock.expectLastCall().andAnswer(() -> {
            callback.getValue().onComplete(offsets, null);
            return null;
        });

        expectConsumerPoll(0);
        sinkTask.put(Collections.emptyList());
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();

        // Initial assignment
        time.sleep(30000L);
        workerTask.iteration();
        assertSinkMetricValue("partition-count", 2);

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
        assertNotEquals(offsets, Whitebox.<Map<TopicPartition, OffsetAndMetadata>>getInternalState(workerTask, "lastCommittedOffsets"));
        time.sleep(10000L);
        workerTask.iteration(); // triggers the commit
        time.sleep(10000L);
        assertFalse(sinkTaskContext.getValue().isCommitRequested()); // should have been cleared
        assertEquals(offsets, Whitebox.<Map<TopicPartition, OffsetAndMetadata>>getInternalState(workerTask, "lastCommittedOffsets"));
        assertEquals(0, workerTask.commitFailures());
        // Assert the next commit time advances slightly, the amount it advances
        // is the normal commit time less the two sleeps since it started each
        // of those sleeps were 10 seconds.
        // KAFKA-8229
        assertEquals("Should have only advanced by 40 seconds",
                     previousCommitValue  +
                     (WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT - 10000L * 2),
                     workerTask.getNextCommit());

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

        PowerMock.verifyAll();
    }

    @Test
    public void testPreCommit() throws Exception {
        createTask(initialState);

        expectInitializeTask();
        expectTaskGetTopic(true);

        // iter 1
        expectPollInitialAssignment();

        // iter 2
        expectConsumerPoll(2);
        expectConversionAndTransformation(2);
        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall();

        final Map<TopicPartition, OffsetAndMetadata> workerStartingOffsets = new HashMap<>();
        workerStartingOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET));
        workerStartingOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));

        final Map<TopicPartition, OffsetAndMetadata> workerCurrentOffsets = new HashMap<>();
        workerCurrentOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 2));
        workerCurrentOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));

        final Map<TopicPartition, OffsetAndMetadata> taskOffsets = new HashMap<>();
        taskOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1)); // act like FIRST_OFFSET+2 has not yet been flushed by the task
        taskOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET + 1)); // should be ignored because > current offset
        taskOffsets.put(new TopicPartition(TOPIC, 3), new OffsetAndMetadata(FIRST_OFFSET)); // should be ignored because this partition is not assigned

        final Map<TopicPartition, OffsetAndMetadata> committableOffsets = new HashMap<>();
        committableOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        committableOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));

        sinkTask.preCommit(workerCurrentOffsets);
        EasyMock.expectLastCall().andReturn(taskOffsets);
        // Expect extra invalid topic partition to be filtered, which causes the consumer assignment to be logged
        EasyMock.expect(consumer.assignment()).andReturn(INITIAL_ASSIGNMENT).times(2);
        final Capture<OffsetCommitCallback> callback = EasyMock.newCapture();
        consumer.commitAsync(EasyMock.eq(committableOffsets), EasyMock.capture(callback));
        EasyMock.expectLastCall().andAnswer(() -> {
            callback.getValue().onComplete(committableOffsets, null);
            return null;
        });
        expectConsumerPoll(0);
        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration(); // iter 1 -- initial assignment

        assertEquals(workerStartingOffsets, Whitebox.<Map<TopicPartition, OffsetAndMetadata>>getInternalState(workerTask, "currentOffsets"));
        workerTask.iteration(); // iter 2 -- deliver 2 records

        assertEquals(workerCurrentOffsets, Whitebox.<Map<TopicPartition, OffsetAndMetadata>>getInternalState(workerTask, "currentOffsets"));
        assertEquals(workerStartingOffsets, Whitebox.<Map<TopicPartition, OffsetAndMetadata>>getInternalState(workerTask, "lastCommittedOffsets"));
        sinkTaskContext.getValue().requestCommit();
        workerTask.iteration(); // iter 3 -- commit
        assertEquals(committableOffsets, Whitebox.<Map<TopicPartition, OffsetAndMetadata>>getInternalState(workerTask, "lastCommittedOffsets"));

        PowerMock.verifyAll();
    }

    @Test
    public void testPreCommitFailure() throws Exception {
        createTask(initialState);

        expectInitializeTask();
        expectTaskGetTopic(true);
        EasyMock.expect(consumer.assignment()).andStubReturn(INITIAL_ASSIGNMENT);

        // iter 1
        expectPollInitialAssignment();

        // iter 2
        expectConsumerPoll(2);
        expectConversionAndTransformation(2);
        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall();

        // iter 3
        final Map<TopicPartition, OffsetAndMetadata> workerCurrentOffsets = new HashMap<>();
        workerCurrentOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 2));
        workerCurrentOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));
        sinkTask.preCommit(workerCurrentOffsets);
        EasyMock.expectLastCall().andThrow(new ConnectException("Failed to flush"));

        consumer.seek(TOPIC_PARTITION, FIRST_OFFSET);
        EasyMock.expectLastCall();
        consumer.seek(TOPIC_PARTITION2, FIRST_OFFSET);
        EasyMock.expectLastCall();

        expectConsumerPoll(0);
        sinkTask.put(EasyMock.eq(Collections.emptyList()));
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration(); // iter 1 -- initial assignment
        workerTask.iteration(); // iter 2 -- deliver 2 records
        sinkTaskContext.getValue().requestCommit();
        workerTask.iteration(); // iter 3 -- commit

        PowerMock.verifyAll();
    }

    @Test
    public void testIgnoredCommit() throws Exception {
        createTask(initialState);

        expectInitializeTask();
        expectTaskGetTopic(true);

        // iter 1
        expectPollInitialAssignment();

        // iter 2
        expectConsumerPoll(1);
        expectConversionAndTransformation(1);
        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall();

        final Map<TopicPartition, OffsetAndMetadata> workerStartingOffsets = new HashMap<>();
        workerStartingOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET));
        workerStartingOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));

        final Map<TopicPartition, OffsetAndMetadata> workerCurrentOffsets = new HashMap<>();
        workerCurrentOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        workerCurrentOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));

        EasyMock.expect(consumer.assignment()).andReturn(INITIAL_ASSIGNMENT).times(2);

        // iter 3
        sinkTask.preCommit(workerCurrentOffsets);
        EasyMock.expectLastCall().andReturn(workerStartingOffsets);
        // no actual consumer.commit() triggered
        expectConsumerPoll(0);
        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration(); // iter 1 -- initial assignment

        assertEquals(workerStartingOffsets, Whitebox.<Map<TopicPartition, OffsetAndMetadata>>getInternalState(workerTask, "currentOffsets"));
        assertEquals(workerStartingOffsets, Whitebox.<Map<TopicPartition, OffsetAndMetadata>>getInternalState(workerTask, "lastCommittedOffsets"));

        workerTask.iteration(); // iter 2 -- deliver 2 records

        sinkTaskContext.getValue().requestCommit();
        workerTask.iteration(); // iter 3 -- commit

        PowerMock.verifyAll();
    }

    // Test that the commitTimeoutMs timestamp is correctly computed and checked in WorkerSinkTask.iteration()
    // when there is a long running commit in process. See KAFKA-4942 for more information.
    @Test
    public void testLongRunningCommitWithoutTimeout() throws Exception {
        createTask(initialState);

        expectInitializeTask();
        expectTaskGetTopic(true);

        // iter 1
        expectPollInitialAssignment();

        // iter 2
        expectConsumerPoll(1);
        expectConversionAndTransformation(1);
        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall();

        final Map<TopicPartition, OffsetAndMetadata> workerStartingOffsets = new HashMap<>();
        workerStartingOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET));
        workerStartingOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));

        final Map<TopicPartition, OffsetAndMetadata> workerCurrentOffsets = new HashMap<>();
        workerCurrentOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        workerCurrentOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));

        EasyMock.expect(consumer.assignment()).andReturn(INITIAL_ASSIGNMENT).times(2);

        // iter 3 - note that we return the current offset to indicate they should be committed
        sinkTask.preCommit(workerCurrentOffsets);
        EasyMock.expectLastCall().andReturn(workerCurrentOffsets);

        // We need to delay the result of trying to commit offsets to Kafka via the consumer.commitAsync
        // method. We do this so that we can test that we do not erroneously mark a commit as timed out
        // while it is still running and under time. To fake this for tests we have the commit run in a
        // separate thread and wait for a latch which we control back in the main thread.
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final CountDownLatch latch = new CountDownLatch(1);

        consumer.commitAsync(EasyMock.eq(workerCurrentOffsets), EasyMock.anyObject());
        EasyMock.expectLastCall().andAnswer(() -> {
            // Grab the arguments passed to the consumer.commitAsync method
            final Object[] args = EasyMock.getCurrentArguments();
            @SuppressWarnings("unchecked")
            final Map<TopicPartition, OffsetAndMetadata> offsets = (Map<TopicPartition, OffsetAndMetadata>) args[0];
            final OffsetCommitCallback callback = (OffsetCommitCallback) args[1];

            executor.execute(() -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                callback.onComplete(offsets, null);
            });

            return null;
        });

        // no actual consumer.commit() triggered
        expectConsumerPoll(0);

        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration(); // iter 1 -- initial assignment

        assertEquals(workerStartingOffsets, Whitebox.<Map<TopicPartition, OffsetAndMetadata>>getInternalState(workerTask, "currentOffsets"));
        assertEquals(workerStartingOffsets, Whitebox.<Map<TopicPartition, OffsetAndMetadata>>getInternalState(workerTask, "lastCommittedOffsets"));

        time.sleep(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_DEFAULT);
        workerTask.iteration(); // iter 2 -- deliver 2 records

        sinkTaskContext.getValue().requestCommit();
        workerTask.iteration(); // iter 3 -- commit in progress

        // Make sure the "committing" flag didn't immediately get flipped back to false due to an incorrect timeout
        assertTrue("Expected worker to be in the process of committing offsets", workerTask.isCommitting());

        // Let the async commit finish and wait for it to end
        latch.countDown();
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        assertEquals(workerCurrentOffsets, Whitebox.<Map<TopicPartition, OffsetAndMetadata>>getInternalState(workerTask, "currentOffsets"));
        assertEquals(workerCurrentOffsets, Whitebox.<Map<TopicPartition, OffsetAndMetadata>>getInternalState(workerTask, "lastCommittedOffsets"));

        PowerMock.verifyAll();
    }

    @Test
    public void testSinkTasksHandleCloseErrors() throws Exception {
        createTask(initialState);
        expectInitializeTask();
        expectTaskGetTopic(true);

        expectPollInitialAssignment();

        // Put one message through the task to get some offsets to commit
        expectConsumerPoll(1);
        expectConversionAndTransformation(1);
        sinkTask.put(EasyMock.anyObject());
        PowerMock.expectLastCall().andVoid();

        // Stop the task during the next put
        expectConsumerPoll(1);
        expectConversionAndTransformation(1);
        sinkTask.put(EasyMock.anyObject());
        PowerMock.expectLastCall().andAnswer(() -> {
            workerTask.stop();
            return null;
        });

        consumer.wakeup();
        PowerMock.expectLastCall();

        // Throw another exception while closing the task's assignment
        EasyMock.expect(sinkTask.preCommit(EasyMock.anyObject()))
            .andStubReturn(Collections.emptyMap());
        Throwable closeException = new RuntimeException();
        sinkTask.close(EasyMock.anyObject());
        PowerMock.expectLastCall().andThrow(closeException);

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        try {
            workerTask.execute();
            fail("workerTask.execute should have thrown an exception");
        } catch (RuntimeException e) {
            PowerMock.verifyAll();
            assertSame("Exception from close should propagate as-is", closeException, e);
        }
    }

    @Test
    public void testSuppressCloseErrors() throws Exception {
        createTask(initialState);
        expectInitializeTask();
        expectTaskGetTopic(true);

        expectPollInitialAssignment();

        // Put one message through the task to get some offsets to commit
        expectConsumerPoll(1);
        expectConversionAndTransformation(1);
        sinkTask.put(EasyMock.anyObject());
        PowerMock.expectLastCall().andVoid();

        // Throw an exception on the next put to trigger shutdown behavior
        // This exception is the true "cause" of the failure
        expectConsumerPoll(1);
        expectConversionAndTransformation(1);
        Throwable putException = new RuntimeException();
        sinkTask.put(EasyMock.anyObject());
        PowerMock.expectLastCall().andThrow(putException);

        // Throw another exception while closing the task's assignment
        EasyMock.expect(sinkTask.preCommit(EasyMock.anyObject()))
            .andStubReturn(Collections.emptyMap());
        Throwable closeException = new RuntimeException();
        sinkTask.close(EasyMock.anyObject());
        PowerMock.expectLastCall().andThrow(closeException);

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        try {
            workerTask.execute();
            fail("workerTask.execute should have thrown an exception");
        } catch (ConnectException e) {
            PowerMock.verifyAll();
            assertSame("Exception from put should be the cause", putException, e.getCause());
            assertTrue("Exception from close should be suppressed", e.getSuppressed().length > 0);
            assertSame(closeException, e.getSuppressed()[0]);
        }
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
        EasyMock.expect(consumer.assignment()).andReturn(new HashSet<>(rebalancedPartitions)).times(6);

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

    @Test
    public void testDeliveryWithMutatingTransform() throws Exception {
        createTask(initialState);

        expectInitializeTask();
        expectTaskGetTopic(true);

        expectPollInitialAssignment();

        expectConsumerPoll(1);
        expectConversionAndTransformation(1, "newtopic_");
        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall();

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        offsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));
        sinkTask.preCommit(offsets);
        EasyMock.expectLastCall().andReturn(offsets);

        EasyMock.expect(consumer.assignment()).andReturn(INITIAL_ASSIGNMENT).times(2);

        final Capture<OffsetCommitCallback> callback = EasyMock.newCapture();
        consumer.commitAsync(EasyMock.eq(offsets), EasyMock.capture(callback));
        EasyMock.expectLastCall().andAnswer(() -> {
            callback.getValue().onComplete(offsets, null);
            return null;
        });

        expectConsumerPoll(0);
        sinkTask.put(Collections.emptyList());
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();

        workerTask.iteration(); // initial assignment

        workerTask.iteration(); // first record delivered

        sinkTaskContext.getValue().requestCommit();
        assertTrue(sinkTaskContext.getValue().isCommitRequested());
        assertNotEquals(offsets, Whitebox.<Map<TopicPartition, OffsetAndMetadata>>getInternalState(workerTask, "lastCommittedOffsets"));
        workerTask.iteration(); // triggers the commit
        assertFalse(sinkTaskContext.getValue().isCommitRequested()); // should have been cleared
        assertEquals(offsets, Whitebox.<Map<TopicPartition, OffsetAndMetadata>>getInternalState(workerTask, "lastCommittedOffsets"));
        assertEquals(0, workerTask.commitFailures());
        assertEquals(1.0, metrics.currentMetricValueAsDouble(workerTask.taskMetricsGroup().metricGroup(), "batch-size-max"), 0.0001);

        PowerMock.verifyAll();
    }

    @Test
    public void testMissingTimestampPropagation() throws Exception {
        createTask(initialState);

        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();
        expectConsumerPoll(1, RecordBatch.NO_TIMESTAMP, TimestampType.CREATE_TIME);
        expectConversionAndTransformation(1);

        Capture<Collection<SinkRecord>> records = EasyMock.newCapture(CaptureType.ALL);

        sinkTask.put(EasyMock.capture(records));

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration(); // iter 1 -- initial assignment
        workerTask.iteration(); // iter 2 -- deliver 1 record

        SinkRecord record = records.getValue().iterator().next();

        // we expect null for missing timestamp, the sentinel value of Record.NO_TIMESTAMP is Kafka's API
        assertNull(record.timestamp());
        assertEquals(TimestampType.CREATE_TIME, record.timestampType());

        PowerMock.verifyAll();
    }

    @Test
    public void testTimestampPropagation() throws Exception {
        final Long timestamp = System.currentTimeMillis();
        final TimestampType timestampType = TimestampType.CREATE_TIME;

        createTask(initialState);

        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();
        expectConsumerPoll(1, timestamp, timestampType);
        expectConversionAndTransformation(1);

        Capture<Collection<SinkRecord>> records = EasyMock.newCapture(CaptureType.ALL);
        sinkTask.put(EasyMock.capture(records));

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration(); // iter 1 -- initial assignment
        workerTask.iteration(); // iter 2 -- deliver 1 record

        SinkRecord record = records.getValue().iterator().next();

        assertEquals(timestamp, record.timestamp());
        assertEquals(timestampType, record.timestampType());

        PowerMock.verifyAll();
    }

    @Test
    public void testTopicsRegex() {
        Map<String, String> props = new HashMap<>(TASK_PROPS);
        props.remove("topics");
        props.put("topics.regex", "te.*");
        TaskConfig taskConfig = new TaskConfig(props);

        createTask(TargetState.PAUSED);

        consumer.subscribe(EasyMock.capture(topicsRegex), EasyMock.capture(rebalanceListener));
        PowerMock.expectLastCall();

        sinkTask.initialize(EasyMock.capture(sinkTaskContext));
        PowerMock.expectLastCall();
        sinkTask.start(props);
        PowerMock.expectLastCall();

        expectPollInitialAssignment();

        EasyMock.expect(consumer.assignment()).andReturn(INITIAL_ASSIGNMENT);
        consumer.pause(INITIAL_ASSIGNMENT);
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(taskConfig);
        workerTask.initializeAndStart();
        workerTask.iteration();
        time.sleep(10000L);

        PowerMock.verifyAll();
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
    public void testHeaders() throws Exception {
        Headers headers = new RecordHeaders();
        headers.add("header_key", "header_value".getBytes());

        createTask(initialState);

        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();

        expectConsumerPoll(1, headers);
        expectConversionAndTransformation(1, null, headers);
        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration(); // iter 1 -- initial assignment
        workerTask.iteration(); // iter 2 -- deliver 1 record

        PowerMock.verifyAll();
    }

    @Test
    public void testHeadersWithCustomConverter() throws Exception {
        StringConverter stringConverter = new StringConverter();
        SampleConverterWithHeaders testConverter = new SampleConverterWithHeaders();

        createTask(initialState, stringConverter, testConverter, stringConverter);

        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();

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

        expectConsumerPoll(Arrays.asList(
            new ConsumerRecord<>(TOPIC, PARTITION, FIRST_OFFSET + recordsReturnedTp1 + 1, RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE,
                0, 0, keyA.getBytes(), valueA.getBytes(encodingA), headersA, Optional.empty()),
            new ConsumerRecord<>(TOPIC, PARTITION, FIRST_OFFSET + recordsReturnedTp1 + 2, RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE,
                0, 0, keyB.getBytes(), valueB.getBytes(encodingB), headersB, Optional.empty())
        ));

        expectTransformation(2, null);

        Capture<Collection<SinkRecord>> records = EasyMock.newCapture(CaptureType.ALL);
        sinkTask.put(EasyMock.capture(records));

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration(); // iter 1 -- initial assignment
        workerTask.iteration(); // iter 2 -- deliver 1 record

        Iterator<SinkRecord> iterator = records.getValue().iterator();

        SinkRecord recordA = iterator.next();
        assertEquals(keyA, recordA.key());
        assertEquals(valueA, recordA.value());

        SinkRecord recordB = iterator.next();
        assertEquals(keyB, recordB.key());
        assertEquals(valueB, recordB.value());

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

    private void expectRebalanceLossError(RuntimeException e) {
        sinkTask.close(new HashSet<>(INITIAL_ASSIGNMENT));
        EasyMock.expectLastCall().andThrow(e);

        EasyMock.expect(consumer.poll(Duration.ofMillis(EasyMock.anyLong()))).andAnswer(
            () -> {
                rebalanceListener.getValue().onPartitionsLost(INITIAL_ASSIGNMENT);
                return ConsumerRecords.empty();
            });
    }

    private void expectRebalanceRevocationError(RuntimeException e) {
        sinkTask.close(new HashSet<>(INITIAL_ASSIGNMENT));
        EasyMock.expectLastCall().andThrow(e);

        sinkTask.preCommit(EasyMock.anyObject());
        EasyMock.expectLastCall().andReturn(Collections.emptyMap());

        EasyMock.expect(consumer.poll(Duration.ofMillis(EasyMock.anyLong()))).andAnswer(
            () -> {
                rebalanceListener.getValue().onPartitionsRevoked(INITIAL_ASSIGNMENT);
                return ConsumerRecords.empty();
            });
    }

    private void expectRebalanceAssignmentError(RuntimeException e) {
        sinkTask.close(INITIAL_ASSIGNMENT);
        EasyMock.expectLastCall();

        sinkTask.preCommit(EasyMock.anyObject());
        EasyMock.expectLastCall().andReturn(Collections.emptyMap());

        EasyMock.expect(consumer.position(TOPIC_PARTITION)).andReturn(FIRST_OFFSET);
        EasyMock.expect(consumer.position(TOPIC_PARTITION2)).andReturn(FIRST_OFFSET);

        sinkTask.open(INITIAL_ASSIGNMENT);
        EasyMock.expectLastCall().andThrow(e);

        EasyMock.expect(consumer.assignment()).andReturn(INITIAL_ASSIGNMENT).times(3);
        EasyMock.expect(consumer.poll(Duration.ofMillis(EasyMock.anyLong()))).andAnswer(
            () -> {
                rebalanceListener.getValue().onPartitionsRevoked(INITIAL_ASSIGNMENT);
                rebalanceListener.getValue().onPartitionsAssigned(INITIAL_ASSIGNMENT);
                return ConsumerRecords.empty();
            });
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

    private void expectConsumerWakeup() {
        consumer.wakeup();
        EasyMock.expectLastCall();
        EasyMock.expect(consumer.poll(Duration.ofMillis(EasyMock.anyLong()))).andThrow(new WakeupException());
    }

    private void expectConsumerPoll(final int numMessages) {
        expectConsumerPoll(numMessages, RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, emptyHeaders());
    }

    private void expectConsumerPoll(final int numMessages, Headers headers) {
        expectConsumerPoll(numMessages, RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, headers);
    }

    private void expectConsumerPoll(final int numMessages, final long timestamp, final TimestampType timestampType) {
        expectConsumerPoll(numMessages, timestamp, timestampType, emptyHeaders());
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

    private void expectConsumerPoll(List<ConsumerRecord<byte[], byte[]>> records) {
        EasyMock.expect(consumer.poll(Duration.ofMillis(EasyMock.anyLong()))).andAnswer(
            () -> new ConsumerRecords<>(
                records.isEmpty() ?
                    Collections.emptyMap() :
                    Collections.singletonMap(new TopicPartition(TOPIC, PARTITION), records)
            ));
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
        EasyMock.expect(transformationChain.apply(EasyMock.capture(recordCapture)))
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

    private void printMetrics() {
        System.out.println();
        sinkMetricValue("sink-record-read-rate");
        sinkMetricValue("sink-record-read-total");
        sinkMetricValue("sink-record-send-rate");
        sinkMetricValue("sink-record-send-total");
        sinkMetricValue("sink-record-active-count");
        sinkMetricValue("sink-record-active-count-max");
        sinkMetricValue("sink-record-active-count-avg");
        sinkMetricValue("partition-count");
        sinkMetricValue("offset-commit-seq-no");
        sinkMetricValue("offset-commit-completion-rate");
        sinkMetricValue("offset-commit-completion-total");
        sinkMetricValue("offset-commit-skip-rate");
        sinkMetricValue("offset-commit-skip-total");
        sinkMetricValue("put-batch-max-time-ms");
        sinkMetricValue("put-batch-avg-time-ms");

        taskMetricValue("status-unassigned");
        taskMetricValue("status-running");
        taskMetricValue("status-paused");
        taskMetricValue("status-failed");
        taskMetricValue("status-destroyed");
        taskMetricValue("running-ratio");
        taskMetricValue("pause-ratio");
        taskMetricValue("offset-commit-max-time-ms");
        taskMetricValue("offset-commit-avg-time-ms");
        taskMetricValue("batch-size-max");
        taskMetricValue("batch-size-avg");
        taskMetricValue("offset-commit-failure-percentage");
        taskMetricValue("offset-commit-success-percentage");
    }

    private double sinkMetricValue(String metricName) {
        MetricGroup sinkTaskGroup = workerTask.sinkTaskMetricsGroup().metricGroup();
        double value = metrics.currentMetricValueAsDouble(sinkTaskGroup, metricName);
        System.out.println("** " + metricName + "=" + value);
        return value;
    }

    private double taskMetricValue(String metricName) {
        MetricGroup taskGroup = workerTask.taskMetricsGroup().metricGroup();
        double value = metrics.currentMetricValueAsDouble(taskGroup, metricName);
        System.out.println("** " + metricName + "=" + value);
        return value;
    }


    private void assertMetrics(int minimumPollCountExpected) {
        MetricGroup sinkTaskGroup = workerTask.sinkTaskMetricsGroup().metricGroup();
        MetricGroup taskGroup = workerTask.taskMetricsGroup().metricGroup();
        double readRate = metrics.currentMetricValueAsDouble(sinkTaskGroup, "sink-record-read-rate");
        double readTotal = metrics.currentMetricValueAsDouble(sinkTaskGroup, "sink-record-read-total");
        double sendRate = metrics.currentMetricValueAsDouble(sinkTaskGroup, "sink-record-send-rate");
        double sendTotal = metrics.currentMetricValueAsDouble(sinkTaskGroup, "sink-record-send-total");
    }

    private RecordHeaders emptyHeaders() {
        return new RecordHeaders();
    }

    private abstract static class TestSinkTask extends SinkTask  {
    }
}
