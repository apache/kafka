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

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.integration.MonitorableSourceConnector;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperatorTest;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.CloseableOffsetStorageReader;
import org.apache.kafka.connect.storage.ConnectorOffsetBackingStore;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.ParameterizedTest;
import org.apache.kafka.connect.util.TopicAdmin;
import org.apache.kafka.connect.util.TopicCreationGroup;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.api.easymock.annotation.MockStrict;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.connect.integration.MonitorableSourceConnector.TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SourceConnectorConfig.TOPIC_CREATION_GROUPS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.EXCLUDE_REGEX_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.INCLUDE_REGEX_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_CREATION_ENABLE_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@PowerMockIgnore({"javax.management.*",
                  "org.apache.log4j.*"})
@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(ParameterizedTest.class)
public class WorkerSourceTaskTest {
    private static final String TOPIC = "topic";
    private static final String OTHER_TOPIC = "other-topic";
    private static final Map<String, Object> PARTITION = Collections.singletonMap("key", "partition".getBytes());
    private static final Map<String, Object> OFFSET = Collections.singletonMap("key", 12);

    // Connect-format data
    private static final Schema KEY_SCHEMA = Schema.INT32_SCHEMA;
    private static final Integer KEY = -1;
    private static final Schema RECORD_SCHEMA = Schema.INT64_SCHEMA;
    private static final Long RECORD = 12L;
    // Serialized data. The actual format of this data doesn't matter -- we just want to see that the right version
    // is used in the right place.
    private static final byte[] SERIALIZED_KEY = "converted-key".getBytes();
    private static final byte[] SERIALIZED_RECORD = "converted-record".getBytes();

    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private ConnectorTaskId taskId = new ConnectorTaskId("job", 0);
    private ConnectorTaskId taskId1 = new ConnectorTaskId("job", 1);
    private WorkerConfig config;
    private SourceConnectorConfig sourceConfig;
    private Plugins plugins;
    private MockConnectMetrics metrics;
    @Mock private SourceTask sourceTask;
    @Mock private Converter keyConverter;
    @Mock private Converter valueConverter;
    @Mock private HeaderConverter headerConverter;
    @Mock private TransformationChain<SourceRecord> transformationChain;
    @Mock private KafkaProducer<byte[], byte[]> producer;
    @Mock private TopicAdmin admin;
    @Mock private CloseableOffsetStorageReader offsetReader;
    @Mock private OffsetStorageWriter offsetWriter;
    @Mock private ConnectorOffsetBackingStore offsetStore;
    @Mock private ClusterConfigState clusterConfigState;
    private WorkerSourceTask workerTask;
    @Mock private Future<RecordMetadata> sendFuture;
    @MockStrict private TaskStatus.Listener statusListener;
    @Mock private StatusBackingStore statusBackingStore;
    @Mock private ErrorHandlingMetrics errorHandlingMetrics;

    private Capture<org.apache.kafka.clients.producer.Callback> producerCallbacks;

    private static final Map<String, String> TASK_PROPS = new HashMap<>();
    static {
        TASK_PROPS.put(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());
    }
    private static final TaskConfig TASK_CONFIG = new TaskConfig(TASK_PROPS);

    private static final List<SourceRecord> RECORDS = Arrays.asList(
            new SourceRecord(PARTITION, OFFSET, "topic", null, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD)
    );

    private boolean enableTopicCreation;

    @ParameterizedTest.Parameters
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    public WorkerSourceTaskTest(boolean enableTopicCreation) {
        this.enableTopicCreation = enableTopicCreation;
    }

    @Before
    public void setup() {
        Map<String, String> workerProps = workerProps();
        plugins = new Plugins(workerProps);
        config = new StandaloneConfig(workerProps);
        sourceConfig = new SourceConnectorConfig(plugins, sourceConnectorPropsWithGroups(TOPIC), true);
        producerCallbacks = EasyMock.newCapture();
        metrics = new MockConnectMetrics();
    }

    private Map<String, String> workerProps() {
        Map<String, String> props = new HashMap<>();
        props.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.put("offset.storage.file.filename", "/tmp/connect.offsets");
        props.put(TOPIC_CREATION_ENABLE_CONFIG, String.valueOf(enableTopicCreation));
        return props;
    }

    private Map<String, String> sourceConnectorPropsWithGroups(String topic) {
        // setup up props for the source connector
        Map<String, String> props = new HashMap<>();
        props.put("name", "foo-connector");
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(1));
        props.put(TOPIC_CONFIG, topic);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(TOPIC_CREATION_GROUPS_CONFIG, String.join(",", "foo", "bar"));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(1));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(1));
        props.put(SourceConnectorConfig.TOPIC_CREATION_PREFIX + "foo" + "." + INCLUDE_REGEX_CONFIG, topic);
        props.put(SourceConnectorConfig.TOPIC_CREATION_PREFIX + "bar" + "." + INCLUDE_REGEX_CONFIG, ".*");
        props.put(SourceConnectorConfig.TOPIC_CREATION_PREFIX + "bar" + "." + EXCLUDE_REGEX_CONFIG, topic);
        return props;
    }

    @After
    public void tearDown() {
        if (metrics != null) metrics.stop();
    }

    private void createWorkerTask() {
        createWorkerTask(TargetState.STARTED, RetryWithToleranceOperatorTest.NOOP_OPERATOR);
    }

    private void createWorkerTaskWithErrorToleration() {
        createWorkerTask(TargetState.STARTED, RetryWithToleranceOperatorTest.ALL_OPERATOR);
    }

    private void createWorkerTask(TargetState initialState) {
        createWorkerTask(initialState, RetryWithToleranceOperatorTest.NOOP_OPERATOR);
    }

    private void createWorkerTask(TargetState initialState, RetryWithToleranceOperator retryWithToleranceOperator) {
        createWorkerTask(initialState, keyConverter, valueConverter, headerConverter, retryWithToleranceOperator);
    }

    private void createWorkerTask(TargetState initialState, Converter keyConverter, Converter valueConverter,
                                  HeaderConverter headerConverter, RetryWithToleranceOperator retryWithToleranceOperator) {
        workerTask = new WorkerSourceTask(taskId, sourceTask, statusListener, initialState, keyConverter, valueConverter, errorHandlingMetrics, headerConverter,
                transformationChain, producer, admin, TopicCreationGroup.configuredGroups(sourceConfig),
                offsetReader, offsetWriter, offsetStore, config, clusterConfigState, metrics, plugins.delegatingLoader(), Time.SYSTEM,
                retryWithToleranceOperator, statusBackingStore, Runnable::run);
    }

    @Test
    public void testStartPaused() throws Exception {
        final CountDownLatch pauseLatch = new CountDownLatch(1);

        createWorkerTask(TargetState.PAUSED);

        statusListener.onPause(taskId);
        EasyMock.expectLastCall().andAnswer(() -> {
            pauseLatch.countDown();
            return null;
        });

        expectClose();

        statusListener.onShutdown(taskId);
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        assertTrue(pauseLatch.await(5, TimeUnit.SECONDS));
        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();

        PowerMock.verifyAll();
    }

    @Test
    public void testPause() throws Exception {
        createWorkerTask();

        expectCleanStartup();

        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch pollLatch = expectPolls(10, count);
        // In this test, we don't flush, so nothing goes any further than the offset writer

        expectTopicCreation(TOPIC);

        statusListener.onPause(taskId);
        EasyMock.expectLastCall();

        sourceTask.stop();
        EasyMock.expectLastCall();

        offsetWriter.offset(PARTITION, OFFSET);
        PowerMock.expectLastCall();
        expectOffsetFlush(true);

        statusListener.onShutdown(taskId);
        EasyMock.expectLastCall();

        expectClose();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);
        assertTrue(awaitLatch(pollLatch));

        workerTask.transitionTo(TargetState.PAUSED);

        int priorCount = count.get();
        Thread.sleep(100);

        // since the transition is observed asynchronously, the count could be off by one loop iteration
        assertTrue(count.get() - priorCount <= 1);

        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();

        PowerMock.verifyAll();
    }

    @Test
    public void testPollsInBackground() throws Exception {
        createWorkerTask();

        expectCleanStartup();

        final CountDownLatch pollLatch = expectPolls(10);
        // In this test, we don't flush, so nothing goes any further than the offset writer

        expectTopicCreation(TOPIC);

        sourceTask.stop();
        EasyMock.expectLastCall();

        offsetWriter.offset(PARTITION, OFFSET);
        PowerMock.expectLastCall();
        expectOffsetFlush(true);

        statusListener.onShutdown(taskId);
        EasyMock.expectLastCall();

        expectClose();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        assertTrue(awaitLatch(pollLatch));
        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();
        assertPollMetrics(10);

        PowerMock.verifyAll();
    }

    @Test
    public void testFailureInPoll() throws Exception {
        createWorkerTask();

        expectCleanStartup();

        final CountDownLatch pollLatch = new CountDownLatch(1);
        final RuntimeException exception = new RuntimeException();
        EasyMock.expect(sourceTask.poll()).andAnswer(() -> {
            pollLatch.countDown();
            throw exception;
        });

        statusListener.onFailure(taskId, exception);
        EasyMock.expectLastCall();

        sourceTask.stop();
        EasyMock.expectLastCall();
        expectEmptyOffsetFlush();

        expectClose();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        assertTrue(awaitLatch(pollLatch));
        //Failure in poll should trigger automatic stop of the task
        assertTrue(workerTask.awaitStop(1000));
        assertShouldSkipCommit();

        taskFuture.get();
        assertPollMetrics(0);

        PowerMock.verifyAll();
    }

    @Test
    public void testFailureInPollAfterCancel() throws Exception {
        createWorkerTask();

        expectCleanStartup();

        final CountDownLatch pollLatch = new CountDownLatch(1);
        final CountDownLatch workerCancelLatch = new CountDownLatch(1);
        final RuntimeException exception = new RuntimeException();
        EasyMock.expect(sourceTask.poll()).andAnswer(() -> {
            pollLatch.countDown();
            assertTrue(awaitLatch(workerCancelLatch));
            throw exception;
        });

        offsetReader.close();
        PowerMock.expectLastCall();

        producer.close(Duration.ZERO);
        PowerMock.expectLastCall();

        sourceTask.stop();
        EasyMock.expectLastCall();
        expectOffsetFlush(true);

        expectClose();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        assertTrue(awaitLatch(pollLatch));
        workerTask.cancel();
        workerCancelLatch.countDown();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();
        assertPollMetrics(0);

        PowerMock.verifyAll();
    }

    @Test
    public void testFailureInPollAfterStop() throws Exception {
        createWorkerTask();

        expectCleanStartup();

        final CountDownLatch pollLatch = new CountDownLatch(1);
        final CountDownLatch workerStopLatch = new CountDownLatch(1);
        final RuntimeException exception = new RuntimeException();
        EasyMock.expect(sourceTask.poll()).andAnswer(() -> {
            pollLatch.countDown();
            assertTrue(awaitLatch(workerStopLatch));
            throw exception;
        });

        statusListener.onShutdown(taskId);
        EasyMock.expectLastCall();

        sourceTask.stop();
        EasyMock.expectLastCall();
        expectOffsetFlush(true);

        expectClose();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        assertTrue(awaitLatch(pollLatch));
        workerTask.stop();
        workerStopLatch.countDown();
        assertTrue(workerTask.awaitStop(1000));
        assertShouldSkipCommit();

        taskFuture.get();
        assertPollMetrics(0);

        PowerMock.verifyAll();
    }

    @Test
    public void testPollReturnsNoRecords() throws Exception {
        // Test that the task handles an empty list of records
        createWorkerTask();

        expectCleanStartup();

        // We'll wait for some data, then trigger a flush
        final CountDownLatch pollLatch = expectEmptyPolls(1, new AtomicInteger());
        expectEmptyOffsetFlush();

        sourceTask.stop();
        EasyMock.expectLastCall();
        expectEmptyOffsetFlush();

        statusListener.onShutdown(taskId);
        EasyMock.expectLastCall();

        expectClose();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        assertTrue(awaitLatch(pollLatch));
        assertTrue(workerTask.commitOffsets());
        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();
        assertPollMetrics(0);

        PowerMock.verifyAll();
    }

    @Test
    public void testCommit() throws Exception {
        // Test that the task commits properly when prompted
        createWorkerTask();

        expectCleanStartup();

        // We'll wait for some data, then trigger a flush
        final CountDownLatch pollLatch = expectPolls(1);
        expectOffsetFlush(true);

        offsetWriter.offset(PARTITION, OFFSET);
        PowerMock.expectLastCall().atLeastOnce();

        expectTopicCreation(TOPIC);

        sourceTask.stop();
        EasyMock.expectLastCall();
        expectEmptyOffsetFlush();

        statusListener.onShutdown(taskId);
        EasyMock.expectLastCall();

        expectClose();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        assertTrue(awaitLatch(pollLatch));
        assertTrue(workerTask.commitOffsets());
        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();
        assertPollMetrics(1);

        PowerMock.verifyAll();
    }

    @Test
    public void testCommitFailure() throws Exception {
        // Test that the task commits properly when prompted
        createWorkerTask();

        expectCleanStartup();

        // We'll wait for some data, then trigger a flush
        final CountDownLatch pollLatch = expectPolls(1);
        expectOffsetFlush(true);

        offsetWriter.offset(PARTITION, OFFSET);
        PowerMock.expectLastCall().atLeastOnce();

        expectTopicCreation(TOPIC);

        sourceTask.stop();
        EasyMock.expectLastCall();
        expectOffsetFlush(false);

        statusListener.onShutdown(taskId);
        EasyMock.expectLastCall();

        expectClose();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        assertTrue(awaitLatch(pollLatch));
        assertTrue(workerTask.commitOffsets());
        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();
        assertPollMetrics(1);

        PowerMock.verifyAll();
    }

    @Test
    public void testSendRecordsRetries() throws Exception {
        createWorkerTask();

        // Differentiate only by Kafka partition so we can reuse conversion expectations
        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, "topic", 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, "topic", 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record3 = new SourceRecord(PARTITION, OFFSET, "topic", 3, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectTopicCreation(TOPIC);

        // First round
        expectSendRecordOnce();
        // Any Producer retriable exception should work here
        expectSendRecordSyncFailure(new org.apache.kafka.common.errors.TimeoutException("retriable sync failure"));

        // Second round
        expectSendRecordOnce();
        expectSendRecordOnce();

        PowerMock.replayAll();

        // Try to send 3, make first pass, second fail. Should save last two
        Whitebox.setInternalState(workerTask, "toSend", Arrays.asList(record1, record2, record3));
        Whitebox.invokeMethod(workerTask, "sendRecords");
        assertEquals(Arrays.asList(record2, record3), Whitebox.getInternalState(workerTask, "toSend"));

        // Next they all succeed
        Whitebox.invokeMethod(workerTask, "sendRecords");
        assertNull(Whitebox.getInternalState(workerTask, "toSend"));

        PowerMock.verifyAll();
    }

    @Test
    public void testSendRecordsProducerCallbackFail() throws Exception {
        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, "topic", 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, "topic", 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectTopicCreation(TOPIC);

        expectSendRecordProducerCallbackFail();
        expectApplyTransformationChain(false);
        expectConvertHeadersAndKeyValue(false);

        PowerMock.replayAll();

        Whitebox.setInternalState(workerTask, "toSend", Arrays.asList(record1, record2));
        assertThrows(ConnectException.class, () -> Whitebox.invokeMethod(workerTask, "sendRecords"));
    }

    @Test
    public void testSendRecordsProducerSendFailsImmediately() {
        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectPreliminaryCalls();
        expectTopicCreation(TOPIC);

        EasyMock.expect(producer.send(EasyMock.anyObject(), EasyMock.anyObject()))
            .andThrow(new KafkaException("Producer closed while send in progress", new InvalidTopicException(TOPIC)));

        PowerMock.replayAll();

        Whitebox.setInternalState(workerTask, "toSend", Arrays.asList(record1, record2));
        assertThrows(ConnectException.class, () -> Whitebox.invokeMethod(workerTask, "sendRecords"));
    }

    @Test
    public void testSendRecordsTaskCommitRecordFail() throws Exception {
        createWorkerTask();

        // Differentiate only by Kafka partition so we can reuse conversion expectations
        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, "topic", 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, "topic", 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record3 = new SourceRecord(PARTITION, OFFSET, "topic", 3, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectTopicCreation(TOPIC);

        // Source task commit record failure will not cause the task to abort
        expectSendRecordOnce();
        expectSendRecordTaskCommitRecordFail(false);
        expectSendRecordOnce();

        PowerMock.replayAll();

        Whitebox.setInternalState(workerTask, "toSend", Arrays.asList(record1, record2, record3));
        Whitebox.invokeMethod(workerTask, "sendRecords");
        assertNull(Whitebox.getInternalState(workerTask, "toSend"));

        PowerMock.verifyAll();
    }

    @Test
    public void testSourceTaskIgnoresProducerException() throws Exception {
        createWorkerTaskWithErrorToleration();
        expectTopicCreation(TOPIC);

        //Use different offsets for each record so we can verify all were committed
        final Map<String, Object> offset2 = Collections.singletonMap("key", 13);

        // send two records
        // record 1 will succeed
        // record 2 will invoke the producer's failure callback, but ignore the exception via retryOperator
        // and no ConnectException will be thrown
        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, offset2, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        expectOffsetFlush(true);
        expectSendRecordOnce();
        expectSendRecordProducerCallbackFail();
        sourceTask.commitRecord(EasyMock.anyObject(SourceRecord.class), EasyMock.isNull());

        //As of KAFKA-14079 all offsets should be committed, even for failed records (if ignored)
        //Only the last offset will be passed to the method as everything up to that point is committed
        //Before KAFKA-14079 offset 12 would have been passed and not 13 as it would have been unacked
        offsetWriter.offset(PARTITION, offset2);
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        //Send records and then commit offsets and verify both were committed and no exception
        Whitebox.setInternalState(workerTask, "toSend", Arrays.asList(record1, record2));
        Whitebox.invokeMethod(workerTask, "sendRecords");
        Whitebox.invokeMethod(workerTask, "updateCommittableOffsets");
        workerTask.commitOffsets();

        PowerMock.verifyAll();

        //Double check to make sure all submitted records were cleared
        assertEquals(0, ((SubmittedRecords) Whitebox.getInternalState(workerTask,
                "submittedRecords")).records.size());
    }

    @Test
    public void testSlowTaskStart() throws Exception {
        final CountDownLatch startupLatch = new CountDownLatch(1);
        final CountDownLatch finishStartupLatch = new CountDownLatch(1);

        createWorkerTask();

        offsetStore.start();
        EasyMock.expectLastCall();
        sourceTask.initialize(EasyMock.anyObject(SourceTaskContext.class));
        EasyMock.expectLastCall();
        sourceTask.start(TASK_PROPS);
        EasyMock.expectLastCall().andAnswer(() -> {
            startupLatch.countDown();
            assertTrue(awaitLatch(finishStartupLatch));
            return null;
        });

        statusListener.onStartup(taskId);
        EasyMock.expectLastCall();

        sourceTask.stop();
        EasyMock.expectLastCall();
        expectOffsetFlush(true);

        statusListener.onShutdown(taskId);
        EasyMock.expectLastCall();

        expectClose();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        Future<?> workerTaskFuture = executor.submit(workerTask);

        // Stopping immediately while the other thread has work to do should result in no polling, no offset commits,
        // exiting the work thread immediately, and the stop() method will be invoked in the background thread since it
        // cannot be invoked immediately in the thread trying to stop the task.
        assertTrue(awaitLatch(startupLatch));
        workerTask.stop();
        finishStartupLatch.countDown();
        assertTrue(workerTask.awaitStop(1000));

        workerTaskFuture.get();

        PowerMock.verifyAll();
    }

    @Test
    public void testCancel() {
        createWorkerTask();

        offsetReader.close();
        PowerMock.expectLastCall();

        producer.close(Duration.ZERO);
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.cancel();

        PowerMock.verifyAll();
    }

    private TopicAdmin.TopicCreationResponse createdTopic(String topic) {
        Set<String> created = Collections.singleton(topic);
        Set<String> existing = Collections.emptySet();
        return new TopicAdmin.TopicCreationResponse(created, existing);
    }

    private void expectPreliminaryCalls() {
        expectPreliminaryCalls(TOPIC);
    }

    private void expectPreliminaryCalls(String topic) {
        expectConvertHeadersAndKeyValue(topic, true, emptyHeaders());
        expectApplyTransformationChain(false);
    }

    private CountDownLatch expectEmptyPolls(int minimum, final AtomicInteger count) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(minimum);
        // Note that we stub these to allow any number of calls because the thread will continue to
        // run. The count passed in + latch returned just makes sure we get *at least* that number of
        // calls
        EasyMock.expect(sourceTask.poll())
                .andStubAnswer(() -> {
                    count.incrementAndGet();
                    latch.countDown();
                    Thread.sleep(10);
                    return Collections.emptyList();
                });
        return latch;
    }

    private CountDownLatch expectPolls(int minimum, final AtomicInteger count) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(minimum);
        // Note that we stub these to allow any number of calls because the thread will continue to
        // run. The count passed in + latch returned just makes sure we get *at least* that number of
        // calls
        EasyMock.expect(sourceTask.poll())
                .andStubAnswer(() -> {
                    count.incrementAndGet();
                    latch.countDown();
                    Thread.sleep(10);
                    return RECORDS;
                });
        // Fallout of the poll() call
        expectSendRecordAnyTimes();
        return latch;
    }

    private CountDownLatch expectPolls(int count) throws InterruptedException {
        return expectPolls(count, new AtomicInteger());
    }

    @SuppressWarnings("unchecked")
    private void expectSendRecordSyncFailure(Throwable error) {
        expectConvertHeadersAndKeyValue(false);
        expectApplyTransformationChain(false);

        EasyMock.expect(
            producer.send(EasyMock.anyObject(ProducerRecord.class),
                EasyMock.anyObject(org.apache.kafka.clients.producer.Callback.class)))
            .andThrow(error);
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecordAnyTimes() throws InterruptedException {
        return expectSendRecordTaskCommitRecordSucceed(true);
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecordOnce() throws InterruptedException {
        return expectSendRecordTaskCommitRecordSucceed(false);
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecordProducerCallbackFail() throws InterruptedException {
        return expectSendRecord(TOPIC, false, false, false, true, emptyHeaders());
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecordTaskCommitRecordSucceed(boolean anyTimes) throws InterruptedException {
        return expectSendRecord(TOPIC, anyTimes, true, true, true, emptyHeaders());
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecordTaskCommitRecordFail(boolean anyTimes) throws InterruptedException {
        return expectSendRecord(TOPIC, anyTimes, true, false, true, emptyHeaders());
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecord(
        String topic,
        boolean anyTimes,
        boolean sendSuccess,
        boolean commitSuccess,
        boolean isMockedConverters,
        Headers headers
    ) throws InterruptedException {
        if (isMockedConverters) {
            expectConvertHeadersAndKeyValue(topic, anyTimes, headers);
        }

        expectApplyTransformationChain(anyTimes);

        Capture<ProducerRecord<byte[], byte[]>> sent = EasyMock.newCapture();

        // 1. Converted data passed to the producer, which will need callbacks invoked for flush to work
        IExpectationSetters<Future<RecordMetadata>> expect = EasyMock.expect(
            producer.send(EasyMock.capture(sent),
                EasyMock.capture(producerCallbacks)));
        IAnswer<Future<RecordMetadata>> expectResponse = () -> {
            synchronized (producerCallbacks) {
                for (org.apache.kafka.clients.producer.Callback cb : producerCallbacks.getValues()) {
                    if (sendSuccess) {
                        cb.onCompletion(new RecordMetadata(new TopicPartition("foo", 0), 0, 0,
                            0L, 0, 0), null);
                    } else {
                        cb.onCompletion(null, new TopicAuthorizationException("foo"));
                    }
                }
                producerCallbacks.reset();
            }
            return sendFuture;
        };
        if (anyTimes)
            expect.andStubAnswer(expectResponse);
        else
            expect.andAnswer(expectResponse);

        if (sendSuccess) {
            // 2. As a result of a successful producer send callback, we'll notify the source task of the record commit
            expectTaskCommitRecordWithOffset(anyTimes, commitSuccess);
            expectTaskGetTopic(anyTimes);
        }

        return sent;
    }

    private void expectConvertHeadersAndKeyValue(boolean anyTimes) {
        expectConvertHeadersAndKeyValue(TOPIC, anyTimes, emptyHeaders());
    }

    private void expectConvertHeadersAndKeyValue(String topic, boolean anyTimes, Headers headers) {
        for (Header header : headers) {
            IExpectationSetters<byte[]> convertHeaderExpect = EasyMock.expect(headerConverter.fromConnectHeader(topic, header.key(), Schema.STRING_SCHEMA, new String(header.value())));
            if (anyTimes)
                convertHeaderExpect.andStubReturn(header.value());
            else
                convertHeaderExpect.andReturn(header.value());
        }
        IExpectationSetters<byte[]> convertKeyExpect = EasyMock.expect(keyConverter.fromConnectData(topic, headers, KEY_SCHEMA, KEY));
        if (anyTimes)
            convertKeyExpect.andStubReturn(SERIALIZED_KEY);
        else
            convertKeyExpect.andReturn(SERIALIZED_KEY);
        IExpectationSetters<byte[]> convertValueExpect = EasyMock.expect(valueConverter.fromConnectData(topic, headers, RECORD_SCHEMA, RECORD));
        if (anyTimes)
            convertValueExpect.andStubReturn(SERIALIZED_RECORD);
        else
            convertValueExpect.andReturn(SERIALIZED_RECORD);
    }

    private void expectApplyTransformationChain(boolean anyTimes) {
        final Capture<SourceRecord> recordCapture = EasyMock.newCapture();
        IExpectationSetters<SourceRecord> convertKeyExpect = EasyMock.expect(transformationChain.apply(EasyMock.capture(recordCapture)));
        if (anyTimes)
            convertKeyExpect.andStubAnswer(recordCapture::getValue);
        else
            convertKeyExpect.andAnswer(recordCapture::getValue);
    }

    private void expectTaskCommitRecordWithOffset(boolean anyTimes, boolean succeed) throws InterruptedException {
        sourceTask.commitRecord(EasyMock.anyObject(SourceRecord.class), EasyMock.anyObject(RecordMetadata.class));
        IExpectationSetters<Void> expect = EasyMock.expectLastCall();
        if (!succeed) {
            expect = expect.andThrow(new RuntimeException("Error committing record in source task"));
        }
        if (anyTimes) {
            expect.anyTimes();
        }
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

    private boolean awaitLatch(CountDownLatch latch) {
        try {
            return latch.await(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private void expectOffsetFlush(boolean succeed) throws Exception {
        EasyMock.expect(offsetWriter.beginFlush()).andReturn(true);
        Future<Void> flushFuture = PowerMock.createMock(Future.class);
        EasyMock.expect(offsetWriter.doFlush(EasyMock.anyObject(Callback.class))).andReturn(flushFuture);
        // Should throw for failure
        IExpectationSetters<Void> futureGetExpect = EasyMock.expect(
                flushFuture.get(EasyMock.anyLong(), EasyMock.anyObject(TimeUnit.class)));
        if (succeed) {
            sourceTask.commit();
            EasyMock.expectLastCall();
            futureGetExpect.andReturn(null);
        } else {
            futureGetExpect.andThrow(new TimeoutException());
            offsetWriter.cancelFlush();
            PowerMock.expectLastCall();
        }
    }

    private void expectEmptyOffsetFlush() throws Exception {
        EasyMock.expect(offsetWriter.beginFlush()).andReturn(false);
        sourceTask.commit();
        EasyMock.expectLastCall();
    }

    private void assertPollMetrics(int minimumPollCountExpected) {
        MetricGroup sourceTaskGroup = workerTask.sourceTaskMetricsGroup().metricGroup();
        MetricGroup taskGroup = workerTask.taskMetricsGroup().metricGroup();
        double pollRate = metrics.currentMetricValueAsDouble(sourceTaskGroup, "source-record-poll-rate");
        double pollTotal = metrics.currentMetricValueAsDouble(sourceTaskGroup, "source-record-poll-total");
        if (minimumPollCountExpected > 0) {
            assertEquals(RECORDS.size(), metrics.currentMetricValueAsDouble(taskGroup, "batch-size-max"), 0.000001d);
            assertEquals(RECORDS.size(), metrics.currentMetricValueAsDouble(taskGroup, "batch-size-avg"), 0.000001d);
            assertTrue(pollRate > 0.0d);
        } else {
            assertEquals(0.0d, pollRate, 0.0);
        }
        assertTrue(pollTotal >= minimumPollCountExpected);

        double writeRate = metrics.currentMetricValueAsDouble(sourceTaskGroup, "source-record-write-rate");
        double writeTotal = metrics.currentMetricValueAsDouble(sourceTaskGroup, "source-record-write-total");
        if (minimumPollCountExpected > 0) {
            assertTrue(writeRate > 0.0d);
        } else {
            assertEquals(0.0d, writeRate, 0.0);
        }
        assertTrue(writeTotal >= minimumPollCountExpected);

        double pollBatchTimeMax = metrics.currentMetricValueAsDouble(sourceTaskGroup, "poll-batch-max-time-ms");
        double pollBatchTimeAvg = metrics.currentMetricValueAsDouble(sourceTaskGroup, "poll-batch-avg-time-ms");
        if (minimumPollCountExpected > 0) {
            assertTrue(pollBatchTimeMax >= 0.0d);
        }
        assertTrue(Double.isNaN(pollBatchTimeAvg) || pollBatchTimeAvg > 0.0d);
        double activeCount = metrics.currentMetricValueAsDouble(sourceTaskGroup, "source-record-active-count");
        double activeCountMax = metrics.currentMetricValueAsDouble(sourceTaskGroup, "source-record-active-count-max");
        assertEquals(0, activeCount, 0.000001d);
        if (minimumPollCountExpected > 0) {
            assertEquals(RECORDS.size(), activeCountMax, 0.000001d);
        }
    }

    private RecordHeaders emptyHeaders() {
        return new RecordHeaders();
    }

    private abstract static class TestSourceTask extends SourceTask {
    }

    private void expectCleanStartup() {
        offsetStore.start();
        EasyMock.expectLastCall();
        sourceTask.initialize(EasyMock.anyObject(SourceTaskContext.class));
        EasyMock.expectLastCall();
        sourceTask.start(TASK_PROPS);
        EasyMock.expectLastCall();
        statusListener.onStartup(taskId);
        EasyMock.expectLastCall();
    }

    private void expectClose() {
        producer.close(EasyMock.anyObject(Duration.class));
        EasyMock.expectLastCall();

        admin.close(EasyMock.anyObject(Duration.class));
        EasyMock.expectLastCall();

        transformationChain.close();
        EasyMock.expectLastCall();

        offsetReader.close();
        EasyMock.expectLastCall();

        offsetStore.stop();
        EasyMock.expectLastCall();

        try {
            headerConverter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        EasyMock.expectLastCall();
    }

    private void expectTopicCreation(String topic) {
        if (config.topicCreationEnable()) {
            EasyMock.expect(admin.describeTopics(topic)).andReturn(Collections.emptyMap());
            Capture<NewTopic> newTopicCapture = EasyMock.newCapture();
            EasyMock.expect(admin.createOrFindTopics(EasyMock.capture(newTopicCapture))).andReturn(createdTopic(topic));
        }
    }

    private void assertShouldSkipCommit() {
        assertFalse(workerTask.shouldCommitOffsets());

        LogCaptureAppender.setClassLoggerToTrace(SourceTaskOffsetCommitter.class);
        LogCaptureAppender.setClassLoggerToTrace(WorkerSourceTask.class);
        try (LogCaptureAppender committerAppender = LogCaptureAppender.createAndRegister(SourceTaskOffsetCommitter.class);
             LogCaptureAppender taskAppender = LogCaptureAppender.createAndRegister(WorkerSourceTask.class)) {
            SourceTaskOffsetCommitter.commit(workerTask);
            assertEquals(Collections.emptyList(), taskAppender.getMessages());

            List<String> committerMessages = committerAppender.getMessages();
            assertEquals(1, committerMessages.size());
            assertTrue(committerMessages.get(0).contains("Skipping offset commit"));
        }
    }
}
