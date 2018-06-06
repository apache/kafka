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
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.distributed.ClusterConfigState;
import org.apache.kafka.connect.runtime.WorkerSinkTask.SinkTaskMetricsGroup;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperatorTest;
import org.apache.kafka.connect.runtime.isolation.PluginClassLoader;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
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
    private KafkaConsumer<byte[], byte[]> consumer;
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
        workerProps.put("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter.schemas.enable", "false");
        workerProps.put("internal.value.converter.schemas.enable", "false");
        workerProps.put("offset.storage.file.filename", "/tmp/connect.offsets");
        workerConfig = new StandaloneConfig(workerProps);
        pluginLoader = PowerMock.createMock(PluginClassLoader.class);
        metrics = new MockConnectMetrics(time);
        recordsReturnedTp1 = 0;
        recordsReturnedTp3 = 0;
    }

    private void createTask(TargetState initialState) {
        workerTask = PowerMock.createPartialMock(
                WorkerSinkTask.class, new String[]{"createConsumer"},
                taskId, sinkTask, statusListener, initialState, workerConfig, ClusterConfigState.EMPTY, metrics,
                keyConverter, valueConverter, headerConverter,
                transformationChain, pluginLoader, time,
                RetryWithToleranceOperatorTest.NOOP_OPERATOR);
    }

    @After
    public void tearDown() {
        if (metrics != null) metrics.stop();
    }

    @Test
    public void testStartPaused() throws Exception {
        createTask(TargetState.PAUSED);

        expectInitializeTask();
        expectPollInitialAssignment();

        Set<TopicPartition> partitions = new HashSet<>(asList(TOPIC_PARTITION, TOPIC_PARTITION2));
        EasyMock.expect(consumer.assignment()).andReturn(partitions);
        consumer.pause(partitions);
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
        assertTaskMetricValue("offset-commit-max-time-ms", Double.NEGATIVE_INFINITY);

        PowerMock.verifyAll();
    }

    @Test
    public void testPause() throws Exception {
        createTask(initialState);

        expectInitializeTask();
        expectPollInitialAssignment();

        expectConsumerPoll(1);
        expectConversionAndTransformation(1);
        sinkTask.put(EasyMock.<Collection<SinkRecord>>anyObject());
        EasyMock.expectLastCall();

        Set<TopicPartition> partitions = new HashSet<>(asList(TOPIC_PARTITION, TOPIC_PARTITION2));

        // Pause
        statusListener.onPause(taskId);
        EasyMock.expectLastCall();
        expectConsumerWakeup();
        EasyMock.expect(consumer.assignment()).andReturn(partitions);
        consumer.pause(partitions);
        PowerMock.expectLastCall();

        // Offset commit as requested when pausing; No records returned by consumer.poll()
        sinkTask.preCommit(EasyMock.<Map<TopicPartition, OffsetAndMetadata>>anyObject());
        EasyMock.expectLastCall().andStubReturn(Collections.emptyMap());
        expectConsumerPoll(0);
        sinkTask.put(Collections.<SinkRecord>emptyList());
        EasyMock.expectLastCall();

        // And unpause
        statusListener.onResume(taskId);
        EasyMock.expectLastCall();
        expectConsumerWakeup();
        EasyMock.expect(consumer.assignment()).andReturn(new HashSet<>(asList(TOPIC_PARTITION, TOPIC_PARTITION2)));
        consumer.resume(singleton(TOPIC_PARTITION));
        PowerMock.expectLastCall();
        consumer.resume(singleton(TOPIC_PARTITION2));
        PowerMock.expectLastCall();

        expectConsumerPoll(1);
        expectConversionAndTransformation(1);
        sinkTask.put(EasyMock.<Collection<SinkRecord>>anyObject());
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
        assertTaskMetricValue("offset-commit-max-time-ms", Double.NEGATIVE_INFINITY);
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
    public void testPollRedelivery() throws Exception {
        createTask(initialState);

        expectInitializeTask();
        expectPollInitialAssignment();

        // If a retriable exception is thrown, we should redeliver the same batch, pausing the consumer in the meantime
        expectConsumerPoll(1);
        expectConversionAndTransformation(1);
        Capture<Collection<SinkRecord>> records = EasyMock.newCapture(CaptureType.ALL);
        sinkTask.put(EasyMock.capture(records));
        EasyMock.expectLastCall().andThrow(new RetriableException("retry"));
        // Pause
        HashSet<TopicPartition> partitions = new HashSet<>(asList(TOPIC_PARTITION, TOPIC_PARTITION2));
        EasyMock.expect(consumer.assignment()).andReturn(partitions);
        consumer.pause(partitions);
        PowerMock.expectLastCall();

        // Retry delivery should succeed
        expectConsumerPoll(0);
        sinkTask.put(EasyMock.capture(records));
        EasyMock.expectLastCall();
        // And unpause
        EasyMock.expect(consumer.assignment()).andReturn(partitions);
        consumer.resume(singleton(TOPIC_PARTITION));
        PowerMock.expectLastCall();
        consumer.resume(singleton(TOPIC_PARTITION2));
        PowerMock.expectLastCall();

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
        assertTaskMetricValue("offset-commit-max-time-ms", Double.NEGATIVE_INFINITY);
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

        PowerMock.verifyAll();
    }

    @Test
    public void testErrorInRebalancePartitionRevocation() throws Exception {
        RuntimeException exception = new RuntimeException("Revocation error");

        createTask(initialState);

        expectInitializeTask();
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
    public void testWakeupInCommitSyncCausesRetry() throws Exception {
        createTask(initialState);

        expectInitializeTask();

        expectPollInitialAssignment();

        expectConsumerPoll(1);
        expectConversionAndTransformation(1);
        sinkTask.put(EasyMock.<Collection<SinkRecord>>anyObject());
        EasyMock.expectLastCall();

        final List<TopicPartition> partitions = asList(TOPIC_PARTITION, TOPIC_PARTITION2);

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

        sinkTask.close(new HashSet<>(partitions));
        EasyMock.expectLastCall();

        EasyMock.expect(consumer.position(TOPIC_PARTITION)).andReturn(FIRST_OFFSET);
        EasyMock.expect(consumer.position(TOPIC_PARTITION2)).andReturn(FIRST_OFFSET);

        sinkTask.open(partitions);
        EasyMock.expectLastCall();

        EasyMock.expect(consumer.poll(EasyMock.anyLong())).andAnswer(
                new IAnswer<ConsumerRecords<byte[], byte[]>>() {
                    @Override
                    public ConsumerRecords<byte[], byte[]> answer() throws Throwable {
                        rebalanceListener.getValue().onPartitionsRevoked(partitions);
                        rebalanceListener.getValue().onPartitionsAssigned(partitions);
                        return ConsumerRecords.empty();
                    }
                });

        EasyMock.expect(consumer.assignment()).andReturn(new HashSet<>(partitions));

        consumer.resume(Collections.singleton(TOPIC_PARTITION));
        EasyMock.expectLastCall();

        consumer.resume(Collections.singleton(TOPIC_PARTITION2));
        EasyMock.expectLastCall();

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
    public void testRequestCommit() throws Exception {
        createTask(initialState);

        expectInitializeTask();

        expectPollInitialAssignment();

        expectConsumerPoll(1);
        expectConversionAndTransformation(1);
        sinkTask.put(EasyMock.<Collection<SinkRecord>>anyObject());
        EasyMock.expectLastCall();

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        offsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));
        sinkTask.preCommit(offsets);
        EasyMock.expectLastCall().andReturn(offsets);

        final Capture<OffsetCommitCallback> callback = EasyMock.newCapture();
        consumer.commitAsync(EasyMock.eq(offsets), EasyMock.capture(callback));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() throws Throwable {
                callback.getValue().onComplete(offsets, null);
                return null;
            }
        });

        expectConsumerPoll(0);
        sinkTask.put(Collections.<SinkRecord>emptyList());
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

        sinkTaskContext.getValue().requestCommit();
        assertTrue(sinkTaskContext.getValue().isCommitRequested());
        assertNotEquals(offsets, Whitebox.<Map<TopicPartition, OffsetAndMetadata>>getInternalState(workerTask, "lastCommittedOffsets"));
        time.sleep(10000L);
        workerTask.iteration(); // triggers the commit
        time.sleep(10000L);
        assertFalse(sinkTaskContext.getValue().isCommitRequested()); // should have been cleared
        assertEquals(offsets, Whitebox.<Map<TopicPartition, OffsetAndMetadata>>getInternalState(workerTask, "lastCommittedOffsets"));
        assertEquals(0, workerTask.commitFailures());

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

        // iter 1
        expectPollInitialAssignment();

        // iter 2
        expectConsumerPoll(2);
        expectConversionAndTransformation(2);
        sinkTask.put(EasyMock.<Collection<SinkRecord>>anyObject());
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
        EasyMock.expect(consumer.assignment()).andReturn(workerCurrentOffsets.keySet());
        final Capture<OffsetCommitCallback> callback = EasyMock.newCapture();
        consumer.commitAsync(EasyMock.eq(committableOffsets), EasyMock.capture(callback));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() throws Throwable {
                callback.getValue().onComplete(committableOffsets, null);
                return null;
            }
        });
        expectConsumerPoll(0);
        sinkTask.put(EasyMock.<Collection<SinkRecord>>anyObject());
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
    public void testIgnoredCommit() throws Exception {
        createTask(initialState);

        expectInitializeTask();

        // iter 1
        expectPollInitialAssignment();

        // iter 2
        expectConsumerPoll(1);
        expectConversionAndTransformation(1);
        sinkTask.put(EasyMock.<Collection<SinkRecord>>anyObject());
        EasyMock.expectLastCall();

        final Map<TopicPartition, OffsetAndMetadata> workerStartingOffsets = new HashMap<>();
        workerStartingOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET));
        workerStartingOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));

        final Map<TopicPartition, OffsetAndMetadata> workerCurrentOffsets = new HashMap<>();
        workerCurrentOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        workerCurrentOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));

        // iter 3
        sinkTask.preCommit(workerCurrentOffsets);
        EasyMock.expectLastCall().andReturn(workerStartingOffsets);
        // no actual consumer.commit() triggered
        expectConsumerPoll(0);
        sinkTask.put(EasyMock.<Collection<SinkRecord>>anyObject());
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

        // iter 1
        expectPollInitialAssignment();

        // iter 2
        expectConsumerPoll(1);
        expectConversionAndTransformation(1);
        sinkTask.put(EasyMock.<Collection<SinkRecord>>anyObject());
        EasyMock.expectLastCall();

        final Map<TopicPartition, OffsetAndMetadata> workerStartingOffsets = new HashMap<>();
        workerStartingOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET));
        workerStartingOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));

        final Map<TopicPartition, OffsetAndMetadata> workerCurrentOffsets = new HashMap<>();
        workerCurrentOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        workerCurrentOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));

        // iter 3 - note that we return the current offset to indicate they should be committed
        sinkTask.preCommit(workerCurrentOffsets);
        EasyMock.expectLastCall().andReturn(workerCurrentOffsets);

        // We need to delay the result of trying to commit offsets to Kafka via the consumer.commitAsync
        // method. We do this so that we can test that we do not erroneously mark a commit as timed out
        // while it is still running and under time. To fake this for tests we have the commit run in a
        // separate thread and wait for a latch which we control back in the main thread.
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final CountDownLatch latch = new CountDownLatch(1);

        consumer.commitAsync(EasyMock.eq(workerCurrentOffsets), EasyMock.<OffsetCommitCallback>anyObject());
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @SuppressWarnings("unchecked")
            @Override
            public Void answer() throws Throwable {
                // Grab the arguments passed to the consumer.commitAsync method
                final Object[] args = EasyMock.getCurrentArguments();
                final Map<TopicPartition, OffsetAndMetadata> offsets = (Map<TopicPartition, OffsetAndMetadata>) args[0];
                final OffsetCommitCallback callback = (OffsetCommitCallback) args[1];

                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            latch.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }

                        callback.onComplete(offsets, null);
                    }
                });

                return null;
            }
        });

        // no actual consumer.commit() triggered
        expectConsumerPoll(0);

        sinkTask.put(EasyMock.<Collection<SinkRecord>>anyObject());
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

    // Verify that when commitAsync is called but the supplied callback is not called by the consumer before a
    // rebalance occurs, the async callback does not reset the last committed offset from the rebalance.
    // See KAFKA-5731 for more information.
    @Test
    public void testCommitWithOutOfOrderCallback() throws Exception {
        createTask(initialState);

        expectInitializeTask();

        // iter 1
        expectPollInitialAssignment();

        // iter 2
        expectConsumerPoll(1);
        expectConversionAndTransformation(4);
        sinkTask.put(EasyMock.<Collection<SinkRecord>>anyObject());
        EasyMock.expectLastCall();

        final Map<TopicPartition, OffsetAndMetadata> workerStartingOffsets = new HashMap<>();
        workerStartingOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET));
        workerStartingOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));

        final Map<TopicPartition, OffsetAndMetadata> workerCurrentOffsets = new HashMap<>();
        workerCurrentOffsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        workerCurrentOffsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));

        final List<TopicPartition> originalPartitions = asList(TOPIC_PARTITION, TOPIC_PARTITION2);
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
        sinkTask.preCommit(workerCurrentOffsets);
        EasyMock.expectLastCall().andReturn(workerCurrentOffsets);

        // We need to delay the result of trying to commit offsets to Kafka via the consumer.commitAsync
        // method. We do this so that we can test that the callback is not called until after the rebalance
        // changes the lastCommittedOffsets. To fake this for tests we have the commitAsync build a function
        // that will call the callback with the appropriate parameters, and we'll run that function later.
        final AtomicReference<Runnable> asyncCallbackRunner = new AtomicReference<>();
        final AtomicBoolean asyncCallbackRan = new AtomicBoolean();

        consumer.commitAsync(EasyMock.eq(workerCurrentOffsets), EasyMock.<OffsetCommitCallback>anyObject());
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @SuppressWarnings("unchecked")
            @Override
            public Void answer() throws Throwable {
                // Grab the arguments passed to the consumer.commitAsync method
                final Object[] args = EasyMock.getCurrentArguments();
                final Map<TopicPartition, OffsetAndMetadata> offsets = (Map<TopicPartition, OffsetAndMetadata>) args[0];
                final OffsetCommitCallback callback = (OffsetCommitCallback) args[1];
                asyncCallbackRunner.set(new Runnable() {
                    @Override
                    public void run() {
                        callback.onComplete(offsets, null);
                        asyncCallbackRan.set(true);
                    }
                });
                return null;
            }
        });

        // Expect the next poll to discover and perform the rebalance, THEN complete the previous callback handler,
        // and then return one record for TP1 and one for TP3.
        final AtomicBoolean rebalanced = new AtomicBoolean();
        EasyMock.expect(consumer.poll(EasyMock.anyLong())).andAnswer(
                new IAnswer<ConsumerRecords<byte[], byte[]>>() {
                    @Override
                    public ConsumerRecords<byte[], byte[]> answer() throws Throwable {
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
                        records.add(new ConsumerRecord<>(TOPIC, PARTITION, FIRST_OFFSET + recordsReturnedTp1 + 1, timestamp, timestampType, 0L, 0, 0, RAW_KEY, RAW_VALUE));
                        records.add(new ConsumerRecord<>(TOPIC, PARTITION3, FIRST_OFFSET + recordsReturnedTp3 + 1, timestamp, timestampType, 0L, 0, 0, RAW_KEY, RAW_VALUE));
                        recordsReturnedTp1 += 1;
                        recordsReturnedTp3 += 1;
                        return new ConsumerRecords<>(Collections.singletonMap(new TopicPartition(TOPIC, PARTITION), records));
                    }
                });

        // onPartitionsRevoked
        sinkTask.preCommit(workerCurrentOffsets);
        EasyMock.expectLastCall().andReturn(workerCurrentOffsets);
        sinkTask.put(EasyMock.<Collection<SinkRecord>>anyObject());
        EasyMock.expectLastCall();
        sinkTask.close(workerCurrentOffsets.keySet());
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

        // onPartitionsAssigned - step 2
        sinkTask.open(rebalancedPartitions);
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
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() throws Throwable {
                callback.getValue().onComplete(postRebalanceCurrentOffsets, null);
                return null;
            }
        });

        // no actual consumer.commit() triggered
        expectConsumerPoll(1);

        sinkTask.put(EasyMock.<Collection<SinkRecord>>anyObject());
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

        expectPollInitialAssignment();

        expectConsumerPoll(1);
        expectConversionAndTransformation(1, "newtopic_");
        sinkTask.put(EasyMock.<Collection<SinkRecord>>anyObject());
        EasyMock.expectLastCall();

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(TOPIC_PARTITION, new OffsetAndMetadata(FIRST_OFFSET + 1));
        offsets.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));
        sinkTask.preCommit(offsets);
        EasyMock.expectLastCall().andReturn(offsets);

        final Capture<OffsetCommitCallback> callback = EasyMock.newCapture();
        consumer.commitAsync(EasyMock.eq(offsets), EasyMock.capture(callback));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() throws Throwable {
                callback.getValue().onComplete(offsets, null);
                return null;
            }
        });

        expectConsumerPoll(0);
        sinkTask.put(Collections.<SinkRecord>emptyList());
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
        assertEquals(null, record.timestamp());
        assertEquals(TimestampType.CREATE_TIME, record.timestampType());

        PowerMock.verifyAll();
    }

    @Test
    public void testTimestampPropagation() throws Exception {
        final Long timestamp = System.currentTimeMillis();
        final TimestampType timestampType = TimestampType.CREATE_TIME;

        createTask(initialState);

        expectInitializeTask();
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
    public void testTopicsRegex() throws Exception {
        Map<String, String> props = new HashMap<>(TASK_PROPS);
        props.remove("topics");
        props.put("topics.regex", "te.*");
        TaskConfig taskConfig = new TaskConfig(props);

        createTask(TargetState.PAUSED);

        PowerMock.expectPrivate(workerTask, "createConsumer").andReturn(consumer);
        consumer.subscribe(EasyMock.capture(topicsRegex), EasyMock.capture(rebalanceListener));
        PowerMock.expectLastCall();

        sinkTask.initialize(EasyMock.capture(sinkTaskContext));
        PowerMock.expectLastCall();
        sinkTask.start(props);
        PowerMock.expectLastCall();

        expectPollInitialAssignment();

        Set<TopicPartition> partitions = new HashSet<>(asList(TOPIC_PARTITION, TOPIC_PARTITION2));
        EasyMock.expect(consumer.assignment()).andReturn(partitions);
        consumer.pause(partitions);
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

    private void expectInitializeTask() throws Exception {
        PowerMock.expectPrivate(workerTask, "createConsumer").andReturn(consumer);
        consumer.subscribe(EasyMock.eq(asList(TOPIC)), EasyMock.capture(rebalanceListener));
        PowerMock.expectLastCall();

        sinkTask.initialize(EasyMock.capture(sinkTaskContext));
        PowerMock.expectLastCall();
        sinkTask.start(TASK_PROPS);
        PowerMock.expectLastCall();
    }

    private void expectRebalanceRevocationError(RuntimeException e) {
        final List<TopicPartition> partitions = asList(TOPIC_PARTITION, TOPIC_PARTITION2);

        sinkTask.close(new HashSet<>(partitions));
        EasyMock.expectLastCall().andThrow(e);

        sinkTask.preCommit(EasyMock.<Map<TopicPartition, OffsetAndMetadata>>anyObject());
        EasyMock.expectLastCall().andReturn(Collections.emptyMap());

        EasyMock.expect(consumer.poll(EasyMock.anyLong())).andAnswer(
                new IAnswer<ConsumerRecords<byte[], byte[]>>() {
                    @Override
                    public ConsumerRecords<byte[], byte[]> answer() throws Throwable {
                        rebalanceListener.getValue().onPartitionsRevoked(partitions);
                        return ConsumerRecords.empty();
                    }
                });
    }

    private void expectRebalanceAssignmentError(RuntimeException e) {
        final List<TopicPartition> partitions = asList(TOPIC_PARTITION, TOPIC_PARTITION2);

        sinkTask.close(new HashSet<>(partitions));
        EasyMock.expectLastCall();

        sinkTask.preCommit(EasyMock.<Map<TopicPartition, OffsetAndMetadata>>anyObject());
        EasyMock.expectLastCall().andReturn(Collections.emptyMap());

        EasyMock.expect(consumer.position(TOPIC_PARTITION)).andReturn(FIRST_OFFSET);
        EasyMock.expect(consumer.position(TOPIC_PARTITION2)).andReturn(FIRST_OFFSET);

        sinkTask.open(partitions);
        EasyMock.expectLastCall().andThrow(e);

        EasyMock.expect(consumer.poll(EasyMock.anyLong())).andAnswer(
                new IAnswer<ConsumerRecords<byte[], byte[]>>() {
                    @Override
                    public ConsumerRecords<byte[], byte[]> answer() throws Throwable {
                        rebalanceListener.getValue().onPartitionsRevoked(partitions);
                        rebalanceListener.getValue().onPartitionsAssigned(partitions);
                        return ConsumerRecords.empty();
                    }
                });
    }

    private void expectPollInitialAssignment() {
        final List<TopicPartition> partitions = asList(TOPIC_PARTITION, TOPIC_PARTITION2);

        sinkTask.open(partitions);
        EasyMock.expectLastCall();

        EasyMock.expect(consumer.poll(EasyMock.anyLong())).andAnswer(new IAnswer<ConsumerRecords<byte[], byte[]>>() {
            @Override
            public ConsumerRecords<byte[], byte[]> answer() throws Throwable {
                rebalanceListener.getValue().onPartitionsAssigned(partitions);
                return ConsumerRecords.empty();
            }
        });
        EasyMock.expect(consumer.position(TOPIC_PARTITION)).andReturn(FIRST_OFFSET);
        EasyMock.expect(consumer.position(TOPIC_PARTITION2)).andReturn(FIRST_OFFSET);

        sinkTask.put(Collections.<SinkRecord>emptyList());
        EasyMock.expectLastCall();
    }

    private void expectConsumerWakeup() {
        consumer.wakeup();
        EasyMock.expectLastCall();
        EasyMock.expect(consumer.poll(EasyMock.anyLong())).andThrow(new WakeupException());
    }

    private void expectConsumerPoll(final int numMessages) {
        expectConsumerPoll(numMessages, RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE);
    }

    private void expectConsumerPoll(final int numMessages, final long timestamp, final TimestampType timestampType) {
        EasyMock.expect(consumer.poll(EasyMock.anyLong())).andAnswer(
                new IAnswer<ConsumerRecords<byte[], byte[]>>() {
                    @Override
                    public ConsumerRecords<byte[], byte[]> answer() throws Throwable {
                        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
                        for (int i = 0; i < numMessages; i++)
                            records.add(new ConsumerRecord<>(TOPIC, PARTITION, FIRST_OFFSET + recordsReturnedTp1 + i, timestamp, timestampType, 0L, 0, 0, RAW_KEY, RAW_VALUE));
                        recordsReturnedTp1 += numMessages;
                        return new ConsumerRecords<>(
                                numMessages > 0 ?
                                        Collections.singletonMap(new TopicPartition(TOPIC, PARTITION), records) :
                                        Collections.<TopicPartition, List<ConsumerRecord<byte[], byte[]>>>emptyMap()
                        );
                    }
                });
    }

    private void expectConversionAndTransformation(final int numMessages) {
        expectConversionAndTransformation(numMessages, null);
    }

    private void expectConversionAndTransformation(final int numMessages, final String topicPrefix) {
        EasyMock.expect(keyConverter.toConnectData(TOPIC, RAW_KEY)).andReturn(new SchemaAndValue(KEY_SCHEMA, KEY)).times(numMessages);
        EasyMock.expect(valueConverter.toConnectData(TOPIC, RAW_VALUE)).andReturn(new SchemaAndValue(VALUE_SCHEMA, VALUE)).times(numMessages);

        final Capture<SinkRecord> recordCapture = EasyMock.newCapture();
        EasyMock.expect(transformationChain.apply(EasyMock.capture(recordCapture)))
                .andAnswer(new IAnswer<SinkRecord>() {
                    @Override
                    public SinkRecord answer() {
                        SinkRecord origRecord = recordCapture.getValue();
                        return topicPrefix != null && !topicPrefix.isEmpty()
                               ? origRecord.newRecord(
                                       topicPrefix + origRecord.topic(),
                                       origRecord.kafkaPartition(),
                                       origRecord.keySchema(),
                                       origRecord.key(),
                                       origRecord.valueSchema(),
                                       origRecord.value(),
                                       origRecord.timestamp()
                               )
                               : origRecord;
                    }
                }).times(numMessages);
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
        System.out.println("");
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

    private abstract static class TestSinkTask extends SinkTask  {
    }
}
