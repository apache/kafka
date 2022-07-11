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
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.integration.MonitorableSourceConnector;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperatorTest;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.source.TransactionContext;
import org.apache.kafka.connect.storage.CloseableOffsetStorageReader;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.storage.ConnectorOffsetBackingStore;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.ParameterizedTest;
import org.apache.kafka.connect.util.ThreadedTest;
import org.apache.kafka.connect.util.TopicAdmin;
import org.apache.kafka.connect.util.TopicCreationGroup;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.api.easymock.annotation.MockStrict;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.connect.integration.MonitorableSourceConnector.TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SourceConnectorConfig.TOPIC_CREATION_GROUPS_CONFIG;
import static org.apache.kafka.connect.runtime.SourceConnectorConfig.TRANSACTION_BOUNDARY_CONFIG;
import static org.apache.kafka.connect.runtime.SourceConnectorConfig.TRANSACTION_BOUNDARY_INTERVAL_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.EXCLUDE_REGEX_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.INCLUDE_REGEX_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_CREATION_ENABLE_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@PowerMockIgnore({"javax.management.*",
        "org.apache.log4j.*"})
@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(ParameterizedTest.class)
public class ExactlyOnceWorkerSourceTaskTest extends ThreadedTest {
    private static final String TOPIC = "topic";
    private static final Map<String, byte[]> PARTITION = Collections.singletonMap("key", "partition".getBytes());
    private static final Map<String, Integer> OFFSET = Collections.singletonMap("key", 12);

    // Connect-format data
    private static final Schema KEY_SCHEMA = Schema.INT32_SCHEMA;
    private static final Integer KEY = -1;
    private static final Schema RECORD_SCHEMA = Schema.INT64_SCHEMA;
    private static final Long RECORD = 12L;
    // Serialized data. The actual format of this data doesn't matter -- we just want to see that the right version
    // is used in the right place.
    private static final byte[] SERIALIZED_KEY = "converted-key".getBytes();
    private static final byte[] SERIALIZED_RECORD = "converted-record".getBytes();

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ConnectorTaskId taskId = new ConnectorTaskId("job", 0);
    private WorkerConfig config;
    private SourceConnectorConfig sourceConfig;
    private Plugins plugins;
    private MockConnectMetrics metrics;
    private Time time;
    private CountDownLatch pollLatch;
    @Mock private SourceTask sourceTask;
    @Mock private Converter keyConverter;
    @Mock private Converter valueConverter;
    @Mock private HeaderConverter headerConverter;
    @Mock private TransformationChain<SourceRecord> transformationChain;
    @Mock private KafkaProducer<byte[], byte[]> producer;
    @Mock private TopicAdmin admin;
    @Mock private CloseableOffsetStorageReader offsetReader;
    @Mock private OffsetStorageWriter offsetWriter;
    @Mock private ClusterConfigState clusterConfigState;
    private ExactlyOnceWorkerSourceTask workerTask;
    @Mock private Future<RecordMetadata> sendFuture;
    @MockStrict private TaskStatus.Listener statusListener;
    @Mock private StatusBackingStore statusBackingStore;
    @Mock private ConnectorOffsetBackingStore offsetStore;
    @Mock private Runnable preProducerCheck;
    @Mock private Runnable postProducerCheck;

    private Capture<org.apache.kafka.clients.producer.Callback> producerCallbacks;

    private static final Map<String, String> TASK_PROPS = new HashMap<>();
    static {
        TASK_PROPS.put(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());
    }
    private static final TaskConfig TASK_CONFIG = new TaskConfig(TASK_PROPS);

    private static final SourceRecord SOURCE_RECORD =
            new SourceRecord(PARTITION, OFFSET, "topic", null, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

    private static final List<SourceRecord> RECORDS = Collections.singletonList(SOURCE_RECORD);

    private final boolean enableTopicCreation;

    @ParameterizedTest.Parameters
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    public ExactlyOnceWorkerSourceTaskTest(boolean enableTopicCreation) {
        this.enableTopicCreation = enableTopicCreation;
    }

    @Override
    public void setup() {
        super.setup();
        Map<String, String> workerProps = workerProps();
        plugins = new Plugins(workerProps);
        config = new StandaloneConfig(workerProps);
        sourceConfig = new SourceConnectorConfig(plugins, sourceConnectorProps(), true);
        producerCallbacks = EasyMock.newCapture();
        metrics = new MockConnectMetrics();
        time = Time.SYSTEM;
        EasyMock.expect(offsetStore.primaryOffsetsTopic()).andStubReturn("offsets-topic");
        pollLatch = new CountDownLatch(1);
    }

    private Map<String, String> workerProps() {
        Map<String, String> props = new HashMap<>();
        props.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.put("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.put("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.put("internal.key.converter.schemas.enable", "false");
        props.put("internal.value.converter.schemas.enable", "false");
        props.put("offset.storage.file.filename", "/tmp/connect.offsets");
        props.put(TOPIC_CREATION_ENABLE_CONFIG, String.valueOf(enableTopicCreation));
        return props;
    }

    private Map<String, String> sourceConnectorProps() {
        return sourceConnectorProps(SourceTask.TransactionBoundary.DEFAULT);
    }

    private Map<String, String> sourceConnectorProps(SourceTask.TransactionBoundary transactionBoundary) {
        // setup up props for the source connector
        Map<String, String> props = new HashMap<>();
        props.put("name", "foo-connector");
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(1));
        props.put(TOPIC_CONFIG, TOPIC);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(TOPIC_CREATION_GROUPS_CONFIG, String.join(",", "foo", "bar"));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(1));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(1));
        props.put(TRANSACTION_BOUNDARY_CONFIG, transactionBoundary.toString());
        props.put(SourceConnectorConfig.TOPIC_CREATION_PREFIX + "foo" + "." + INCLUDE_REGEX_CONFIG, TOPIC);
        props.put(SourceConnectorConfig.TOPIC_CREATION_PREFIX + "bar" + "." + INCLUDE_REGEX_CONFIG, ".*");
        props.put(SourceConnectorConfig.TOPIC_CREATION_PREFIX + "bar" + "." + EXCLUDE_REGEX_CONFIG, TOPIC);
        return props;
    }

    @After
    public void tearDown() {
        if (metrics != null) metrics.stop();
    }

    private void createWorkerTask() {
        createWorkerTask(TargetState.STARTED);
    }

    private void createWorkerTask(TargetState initialState) {
        createWorkerTask(initialState, keyConverter, valueConverter, headerConverter);
    }

    private void createWorkerTask(TargetState initialState, Converter keyConverter, Converter valueConverter, HeaderConverter headerConverter) {
        workerTask = new ExactlyOnceWorkerSourceTask(taskId, sourceTask, statusListener, initialState, keyConverter, valueConverter, headerConverter,
                transformationChain, producer, admin, TopicCreationGroup.configuredGroups(sourceConfig), offsetReader, offsetWriter, offsetStore,
                config, clusterConfigState, metrics, plugins.delegatingLoader(), time, RetryWithToleranceOperatorTest.NOOP_OPERATOR, statusBackingStore,
                sourceConfig, Runnable::run, preProducerCheck, postProducerCheck);
    }

    @Test
    public void testStartPaused() throws Exception {
        final CountDownLatch pauseLatch = new CountDownLatch(1);

        createWorkerTask(TargetState.PAUSED);

        expectCall(() -> statusListener.onPause(taskId)).andAnswer(() -> {
            pauseLatch.countDown();
            return null;
        });

        // The task checks to see if there are offsets to commit before pausing
        EasyMock.expect(offsetWriter.willFlush()).andReturn(false);

        expectClose();

        expectCall(() -> statusListener.onShutdown(taskId));

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

        expectPreflight();
        expectStartup();

        AtomicInteger polls = new AtomicInteger(0);
        AtomicInteger flushes = new AtomicInteger(0);
        pollLatch = new CountDownLatch(10);
        expectPolls(polls);
        expectAnyFlushes(flushes);

        expectTopicCreation(TOPIC);

        expectCall(() -> statusListener.onPause(taskId));

        expectCall(sourceTask::stop);
        expectCall(() -> statusListener.onShutdown(taskId));

        expectClose();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);
        assertTrue(awaitLatch(pollLatch));

        workerTask.transitionTo(TargetState.PAUSED);

        int priorCount = polls.get();
        Thread.sleep(100);

        // since the transition is observed asynchronously, the count could be off by one loop iteration
        assertTrue(polls.get() - priorCount <= 1);

        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();

        assertEquals("Task should have flushed offsets for every record poll, once on pause, and once for end-of-life offset commit",
                flushes.get(), polls.get() + 2);

        PowerMock.verifyAll();
    }

    @Test
    public void testFailureInPreProducerCheck() {
        createWorkerTask();

        Exception exception = new ConnectException("Failed to perform zombie fencing");
        expectCall(preProducerCheck::run).andThrow(exception);

        expectCall(() -> statusListener.onFailure(taskId, exception));

        // Don't expect task to be stopped since it was never started to begin with

        expectClose();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        // No need to execute on a separate thread; preflight checks should all take place before the poll-send loop starts
        workerTask.run();

        PowerMock.verifyAll();
    }

    @Test
    public void testFailureInOffsetStoreStart() {
        createWorkerTask();

        expectCall(preProducerCheck::run);
        expectCall(producer::initTransactions);
        expectCall(postProducerCheck::run);

        Exception exception = new ConnectException("No soup for you!");
        expectCall(offsetStore::start).andThrow(exception);

        expectCall(() -> statusListener.onFailure(taskId, exception));

        // Don't expect task to be stopped since it was never started to begin with

        expectClose();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        // No need to execute on a separate thread; preflight checks should all take place before the poll-send loop starts
        workerTask.run();

        PowerMock.verifyAll();
    }

    @Test
    public void testFailureInProducerInitialization() {
        createWorkerTask();

        expectCall(preProducerCheck::run);
        expectCall(producer::initTransactions);
        Exception exception = new ConnectException("You can't do that!");
        expectCall(postProducerCheck::run).andThrow(exception);

        expectCall(() -> statusListener.onFailure(taskId, exception));

        // Don't expect task to be stopped since it was never started to begin with

        expectClose();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        // No need to execute on a separate thread; preflight checks should all take place before the poll-send loop starts
        workerTask.run();

        PowerMock.verifyAll();
    }

    @Test
    public void testFailureInPostProducerCheck() {
        createWorkerTask();

        expectCall(preProducerCheck::run);
        Exception exception = new ConnectException("New task configs for the connector have already been generated");
        expectCall(producer::initTransactions).andThrow(exception);

        expectCall(() -> statusListener.onFailure(taskId, exception));

        // Don't expect task to be stopped since it was never started to begin with

        expectClose();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        // No need to execute on a separate thread; preflight checks should all take place before the poll-send loop starts
        workerTask.run();

        PowerMock.verifyAll();
    }

    @Test
    public void testPollsInBackground() throws Exception {
        createWorkerTask();

        expectPreflight();
        expectStartup();

        AtomicInteger polls = new AtomicInteger(0);
        AtomicInteger flushes = new AtomicInteger(0);
        pollLatch = new CountDownLatch(10);
        expectPolls(polls);
        expectAnyFlushes(flushes);

        expectTopicCreation(TOPIC);

        expectCall(sourceTask::stop);
        expectCall(() -> statusListener.onShutdown(taskId));

        expectClose();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        assertTrue(awaitLatch(pollLatch));
        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();
        assertPollMetrics(10);
        assertTransactionMetrics(1);

        assertEquals("Task should have flushed offsets for every record poll and for end-of-life offset commit",
                flushes.get(), polls.get() + 1);

        PowerMock.verifyAll();
    }

    @Test
    public void testFailureInPoll() throws Exception {
        createWorkerTask();

        expectPreflight();
        expectStartup();

        final CountDownLatch pollLatch = new CountDownLatch(1);
        final RuntimeException exception = new RuntimeException();
        EasyMock.expect(sourceTask.poll()).andAnswer(() -> {
            pollLatch.countDown();
            throw exception;
        });

        expectCall(() -> statusListener.onFailure(taskId, exception));
        expectCall(sourceTask::stop);

        expectClose();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        assertTrue(awaitLatch(pollLatch));
        //Failure in poll should trigger automatic stop of the worker
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();
        assertPollMetrics(0);

        PowerMock.verifyAll();
    }

    @Test
    public void testFailureInPollAfterCancel() throws Exception {
        createWorkerTask();

        expectPreflight();
        expectStartup();

        final CountDownLatch pollLatch = new CountDownLatch(1);
        final CountDownLatch workerCancelLatch = new CountDownLatch(1);
        final RuntimeException exception = new RuntimeException();
        EasyMock.expect(sourceTask.poll()).andAnswer(() -> {
            pollLatch.countDown();
            assertTrue(awaitLatch(workerCancelLatch));
            throw exception;
        });

        expectCall(offsetReader::close);
        expectCall(() -> producer.close(Duration.ZERO));
        expectCall(sourceTask::stop);

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

        expectPreflight();
        expectStartup();

        final CountDownLatch pollLatch = new CountDownLatch(1);
        final CountDownLatch workerStopLatch = new CountDownLatch(1);
        final RuntimeException exception = new RuntimeException();
        EasyMock.expect(sourceTask.poll()).andAnswer(() -> {
            pollLatch.countDown();
            assertTrue(awaitLatch(workerStopLatch));
            throw exception;
        });

        expectCall(() -> statusListener.onShutdown(taskId));
        expectCall(sourceTask::stop);

        expectClose();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        assertTrue(awaitLatch(pollLatch));
        workerTask.stop();
        workerStopLatch.countDown();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();
        assertPollMetrics(0);

        PowerMock.verifyAll();
    }

    @Test
    public void testPollReturnsNoRecords() throws Exception {
        // Test that the task handles an empty list of records
        createWorkerTask();

        expectPreflight();
        expectStartup();

        final CountDownLatch pollLatch = expectEmptyPolls(1, new AtomicInteger());
        EasyMock.expect(offsetWriter.willFlush()).andReturn(false).anyTimes();

        expectCall(sourceTask::stop);
        expectCall(() -> statusListener.onShutdown(taskId));

        expectClose();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        assertTrue(awaitLatch(pollLatch));
        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();
        assertPollMetrics(0);

        PowerMock.verifyAll();
    }

    @Test
    public void testPollBasedCommit() throws Exception {
        Map<String, String> connectorProps = sourceConnectorProps(SourceTask.TransactionBoundary.POLL);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);

        createWorkerTask();

        expectPreflight();
        expectStartup();

        AtomicInteger polls = new AtomicInteger();
        AtomicInteger flushes = new AtomicInteger();
        expectPolls(polls);
        expectAnyFlushes(flushes);

        expectTopicCreation(TOPIC);

        expectCall(sourceTask::stop);
        expectCall(() -> statusListener.onShutdown(taskId));

        expectClose();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        assertTrue(awaitLatch(pollLatch));
        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();

        assertEquals("Task should have flushed offsets for every record poll, and for end-of-life offset commit",
                flushes.get(), polls.get() + 1);

        assertPollMetrics(1);
        assertTransactionMetrics(1);

        PowerMock.verifyAll();
    }

    @Test
    public void testIntervalBasedCommit() throws Exception {
        long commitInterval = 618;
        Map<String, String> connectorProps = sourceConnectorProps(SourceTask.TransactionBoundary.INTERVAL);
        connectorProps.put(TRANSACTION_BOUNDARY_INTERVAL_CONFIG, Long.toString(commitInterval));
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);

        time = new MockTime();

        createWorkerTask();

        expectPreflight();
        expectStartup();

        expectPolls();
        final CountDownLatch firstPollLatch = new CountDownLatch(2);
        final CountDownLatch secondPollLatch = new CountDownLatch(2);
        final CountDownLatch thirdPollLatch = new CountDownLatch(2);

        AtomicInteger flushes = new AtomicInteger();
        expectFlush(FlushOutcome.SUCCEED, flushes);
        expectFlush(FlushOutcome.SUCCEED, flushes);
        expectFlush(FlushOutcome.SUCCEED, flushes);

        expectTopicCreation(TOPIC);

        expectCall(sourceTask::stop);
        expectCall(() -> statusListener.onShutdown(taskId));

        expectClose();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        pollLatch = firstPollLatch;
        assertTrue(awaitLatch(pollLatch));
        assertEquals("No flushes should have taken place before offset commit interval has elapsed", 0, flushes.get());
        time.sleep(commitInterval);

        pollLatch = secondPollLatch;
        assertTrue(awaitLatch(pollLatch));
        assertEquals("One flush should have taken place after offset commit interval has elapsed", 1, flushes.get());
        time.sleep(commitInterval * 2);

        pollLatch = thirdPollLatch;
        assertTrue(awaitLatch(pollLatch));
        assertEquals("Two flushes should have taken place after offset commit interval has elapsed again", 2, flushes.get());

        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();

        assertEquals("Task should have flushed offsets twice based on offset commit interval, and performed final end-of-life offset commit",
                3, flushes.get());

        assertPollMetrics(2);

        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorBasedCommit() throws Exception {
        Map<String, String> connectorProps = sourceConnectorProps(SourceTask.TransactionBoundary.CONNECTOR);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        createWorkerTask();

        expectPreflight();
        expectStartup();

        expectPolls();
        List<CountDownLatch> pollLatches = IntStream.range(0, 7).mapToObj(i -> new CountDownLatch(3)).collect(Collectors.toList());

        AtomicInteger flushes = new AtomicInteger();
        // First flush: triggered by TransactionContext::commitTransaction (batch)
        expectFlush(FlushOutcome.SUCCEED, flushes);

        // Second flush: triggered by TransactionContext::commitTransaction (record)
        expectFlush(FlushOutcome.SUCCEED, flushes);

        // Third flush: triggered by TransactionContext::abortTransaction (batch)
        expectCall(producer::abortTransaction);
        EasyMock.expect(offsetWriter.willFlush()).andReturn(true);
        expectFlush(FlushOutcome.SUCCEED, flushes);

        // Third flush: triggered by TransactionContext::abortTransaction (record)
        EasyMock.expect(offsetWriter.willFlush()).andReturn(true);
        expectCall(producer::abortTransaction);
        expectFlush(FlushOutcome.SUCCEED, flushes);

        expectTopicCreation(TOPIC);

        expectCall(sourceTask::stop);
        expectCall(() -> statusListener.onShutdown(taskId));

        expectClose();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        TransactionContext transactionContext = workerTask.sourceTaskContext.transactionContext();

        int poll = -1;
        pollLatch = pollLatches.get(++poll);
        assertTrue(awaitLatch(pollLatch));
        assertEquals("No flushes should have taken place without connector requesting transaction commit", 0, flushes.get());

        transactionContext.commitTransaction();
        pollLatch = pollLatches.get(++poll);
        assertTrue(awaitLatch(pollLatch));
        assertEquals("One flush should have taken place after connector requested batch commit", 1, flushes.get());

        transactionContext.commitTransaction(SOURCE_RECORD);
        pollLatch = pollLatches.get(++poll);
        assertTrue(awaitLatch(pollLatch));
        assertEquals("Two flushes should have taken place after connector requested individual record commit", 2, flushes.get());

        pollLatch = pollLatches.get(++poll);
        assertTrue(awaitLatch(pollLatch));
        assertEquals("Only two flushes should still have taken place without connector re-requesting commit, even on identical records", 2, flushes.get());

        transactionContext.abortTransaction();
        pollLatch = pollLatches.get(++poll);
        assertTrue(awaitLatch(pollLatch));
        assertEquals("Three flushes should have taken place after connector requested batch abort", 3, flushes.get());

        transactionContext.abortTransaction(SOURCE_RECORD);
        pollLatch = pollLatches.get(++poll);
        assertTrue(awaitLatch(pollLatch));
        assertEquals("Four flushes should have taken place after connector requested individual record abort", 4, flushes.get());

        pollLatch = pollLatches.get(++poll);
        assertTrue(awaitLatch(pollLatch));
        assertEquals("Only four flushes should still have taken place without connector re-requesting abort, even on identical records", 4, flushes.get());

        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();

        assertEquals("Task should have flushed offsets four times based on connector-defined boundaries, and skipped final end-of-life offset commit",
                4, flushes.get());

        assertPollMetrics(1);
        assertTransactionMetrics(2);

        PowerMock.verifyAll();
    }

    @Test
    public void testCommitFlushCallbackFailure() throws Exception {
        testCommitFailure(FlushOutcome.FAIL_FLUSH_CALLBACK);
    }

    @Test
    public void testCommitTransactionFailure() throws Exception {
        testCommitFailure(FlushOutcome.FAIL_TRANSACTION_COMMIT);
    }

    private void testCommitFailure(FlushOutcome causeOfFailure) throws Exception {
        createWorkerTask();

        expectPreflight();
        expectStartup();

        expectPolls();
        expectFlush(causeOfFailure);

        expectTopicCreation(TOPIC);

        expectCall(sourceTask::stop);
        // Unlike the standard WorkerSourceTask class, this one fails permanently when offset commits don't succeed
        final CountDownLatch taskFailure = new CountDownLatch(1);
        expectCall(() -> statusListener.onFailure(EasyMock.eq(taskId), EasyMock.anyObject()))
                .andAnswer(() -> {
                    taskFailure.countDown();
                    return null;
                });

        expectClose();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        assertTrue(awaitLatch(taskFailure));
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
        expectSendRecordOnce(false);
        expectCall(producer::beginTransaction);
        // Any Producer retriable exception should work here
        expectSendRecordSyncFailure(new org.apache.kafka.common.errors.TimeoutException("retriable sync failure"));

        // Second round
        expectSendRecordOnce(true);
        expectSendRecordOnce(false);

        PowerMock.replayAll();

        // Try to send 3, make first pass, second fail. Should save last two
        workerTask.toSend = Arrays.asList(record1, record2, record3);
        workerTask.sendRecords();
        assertEquals(Arrays.asList(record2, record3), workerTask.toSend);

        // Next they all succeed
        workerTask.sendRecords();
        assertNull(workerTask.toSend);

        PowerMock.verifyAll();
    }

    @Test
    public void testSendRecordsProducerSendFailsImmediately() {
        if (!enableTopicCreation)
            // should only test with topic creation enabled
            return;

        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectCall(producer::beginTransaction);
        expectTopicCreation(TOPIC);
        expectConvertHeadersAndKeyValue(TOPIC, true, emptyHeaders());
        expectApplyTransformationChain(false);

        EasyMock.expect(producer.send(EasyMock.anyObject(), EasyMock.anyObject()))
                .andThrow(new KafkaException("Producer closed while send in progress", new InvalidTopicException(TOPIC)));

        PowerMock.replayAll();

        workerTask.toSend = Arrays.asList(record1, record2);
        assertThrows(ConnectException.class, workerTask::sendRecords);

        PowerMock.verifyAll();
    }

    @Test
    public void testSlowTaskStart() throws Exception {
        final CountDownLatch startupLatch = new CountDownLatch(1);
        final CountDownLatch finishStartupLatch = new CountDownLatch(1);

        createWorkerTask();

        expectPreflight();

        expectCall(() -> sourceTask.initialize(EasyMock.anyObject(SourceTaskContext.class)));
        expectCall(() -> sourceTask.start(TASK_PROPS));
        EasyMock.expectLastCall().andAnswer(() -> {
            startupLatch.countDown();
            assertTrue(awaitLatch(finishStartupLatch));
            return null;
        });

        expectCall(() -> statusListener.onStartup(taskId));

        expectCall(sourceTask::stop);
        EasyMock.expect(offsetWriter.willFlush()).andReturn(false);

        expectCall(() -> statusListener.onShutdown(taskId));

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

        expectCall(offsetReader::close);
        expectCall(() -> producer.close(Duration.ZERO));

        PowerMock.replayAll();

        // workerTask said something dumb on twitter
        workerTask.cancel();

        PowerMock.verifyAll();
    }

    private TopicAdmin.TopicCreationResponse createdTopic(String topic) {
        Set<String> created = Collections.singleton(topic);
        Set<String> existing = Collections.emptySet();
        return new TopicAdmin.TopicCreationResponse(created, existing);
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

    private void expectPolls(final AtomicInteger pollCount) throws Exception {
        expectCall(producer::beginTransaction).atLeastOnce();
        // Note that we stub these to allow any number of calls because the thread will continue to
        // run. The count passed in + latch returned just makes sure we get *at least* that number of
        // calls
        EasyMock.expect(sourceTask.poll())
                .andStubAnswer(() -> {
                    pollCount.incrementAndGet();
                    pollLatch.countDown();
                    Thread.sleep(10);
                    return RECORDS;
                });
        // Fallout of the poll() call
        expectSendRecordAnyTimes();
    }

    private void expectPolls() throws Exception {
        expectPolls(new AtomicInteger());
    }

    @SuppressWarnings("unchecked")
    private void expectSendRecordSyncFailure(Throwable error) {
        expectConvertHeadersAndKeyValue(false);
        expectApplyTransformationChain(false);

        offsetWriter.offset(PARTITION, OFFSET);
        PowerMock.expectLastCall();

        EasyMock.expect(
                producer.send(EasyMock.anyObject(ProducerRecord.class),
                        EasyMock.anyObject(org.apache.kafka.clients.producer.Callback.class)))
                .andThrow(error);
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecordAnyTimes() {
        return expectSendRecordSendSuccess(true, false);
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecordOnce(boolean isRetry) {
        return expectSendRecordSendSuccess(false, isRetry);
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecordSendSuccess(boolean anyTimes, boolean isRetry) {
        return expectSendRecord(TOPIC, anyTimes, isRetry, true, true, emptyHeaders());
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecord(
            String topic,
            boolean anyTimes,
            boolean isRetry,
            boolean sendSuccess,
            boolean isMockedConverters,
            Headers headers
    ) {
        if (isMockedConverters) {
            expectConvertHeadersAndKeyValue(topic, anyTimes, headers);
        }

        expectApplyTransformationChain(anyTimes);

        Capture<ProducerRecord<byte[], byte[]>> sent = EasyMock.newCapture();

        // 1. Offset data is passed to the offset storage.
        if (!isRetry) {
            offsetWriter.offset(PARTITION, OFFSET);
            if (anyTimes)
                PowerMock.expectLastCall().anyTimes();
            else
                PowerMock.expectLastCall();
        }

        // 2. Converted data passed to the producer, which will need callbacks invoked for flush to work
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
            // 3. As a result of a successful producer send callback, we note the use of the topic
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
                    time.milliseconds()));
        } else {
            expect.andAnswer(() -> new TopicStatus(
                    topicCapture.getValue(),
                    new ConnectorTaskId(connectorCapture.getValue(), 0),
                    time.milliseconds()));
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

    private enum FlushOutcome {
        SUCCEED,
        SUCCEED_ANY_TIMES,
        FAIL_FLUSH_CALLBACK,
        FAIL_TRANSACTION_COMMIT
    }

    private CountDownLatch expectFlush(FlushOutcome outcome, AtomicInteger flushCount) {
        CountDownLatch result = new CountDownLatch(1);
        org.easymock.IExpectationSetters<Boolean> flushBegin = EasyMock
                .expect(offsetWriter.beginFlush())
                .andAnswer(() -> {
                    flushCount.incrementAndGet();
                    result.countDown();
                    return true;
                });
        if (FlushOutcome.SUCCEED_ANY_TIMES.equals(outcome)) {
            flushBegin.anyTimes();
        }

        Capture<Callback<Void>> flushCallback = EasyMock.newCapture();
        org.easymock.IExpectationSetters<Future<Void>> offsetFlush =
                EasyMock.expect(offsetWriter.doFlush(EasyMock.capture(flushCallback)));
        switch (outcome) {
            case SUCCEED:
                // The worker task doesn't actually use the returned future
                offsetFlush.andReturn(null);
                expectCall(producer::commitTransaction);
                expectCall(() -> sourceTask.commitRecord(EasyMock.anyObject(), EasyMock.anyObject()));
                expectCall(sourceTask::commit);
                break;
            case SUCCEED_ANY_TIMES:
                // The worker task doesn't actually use the returned future
                offsetFlush.andReturn(null).anyTimes();
                expectCall(producer::commitTransaction).anyTimes();
                expectCall(() -> sourceTask.commitRecord(EasyMock.anyObject(), EasyMock.anyObject())).anyTimes();
                expectCall(sourceTask::commit).anyTimes();
                break;
            case FAIL_FLUSH_CALLBACK:
                expectCall(producer::commitTransaction);
                offsetFlush.andAnswer(() -> {
                    flushCallback.getValue().onCompletion(new RecordTooLargeException(), null);
                    return null;
                });
                expectCall(offsetWriter::cancelFlush);
                break;
            case FAIL_TRANSACTION_COMMIT:
                offsetFlush.andReturn(null);
                expectCall(producer::commitTransaction)
                        .andThrow(new RecordTooLargeException());
                expectCall(offsetWriter::cancelFlush);
                break;
            default:
                fail("Unexpected flush outcome: " + outcome);
        }
        return result;
    }

    private CountDownLatch expectFlush(FlushOutcome outcome) {
        return expectFlush(outcome, new AtomicInteger());
    }

    private CountDownLatch expectAnyFlushes(AtomicInteger flushCount) {
        EasyMock.expect(offsetWriter.willFlush()).andReturn(true).anyTimes();
        return expectFlush(FlushOutcome.SUCCEED_ANY_TIMES, flushCount);
    }

    private void assertTransactionMetrics(int minimumMaxSizeExpected) {
        MetricGroup transactionGroup = workerTask.transactionMetricsGroup().metricGroup();
        double actualMin = metrics.currentMetricValueAsDouble(transactionGroup, "transaction-size-min");
        double actualMax = metrics.currentMetricValueAsDouble(transactionGroup, "transaction-size-max");
        double actualAvg = metrics.currentMetricValueAsDouble(transactionGroup, "transaction-size-avg");
        assertTrue(actualMin >= 0);
        assertTrue(actualMax >= minimumMaxSizeExpected);

        if (actualMax - actualMin <= 0.000001d) {
            assertEquals(actualMax, actualAvg, 0.000002d);
        } else {
            assertTrue("Average transaction size should be greater than minimum transaction size", actualAvg > actualMin);
            assertTrue("Average transaction size should be less than maximum transaction size", actualAvg < actualMax);
        }
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
            assertTrue(pollRate == 0.0d);
        }
        assertTrue(pollTotal >= minimumPollCountExpected);

        double writeRate = metrics.currentMetricValueAsDouble(sourceTaskGroup, "source-record-write-rate");
        double writeTotal = metrics.currentMetricValueAsDouble(sourceTaskGroup, "source-record-write-total");
        if (minimumPollCountExpected > 0) {
            assertTrue(writeRate > 0.0d);
        } else {
            assertTrue(writeRate == 0.0d);
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

    @FunctionalInterface
    private interface MockedMethodCall {
        void invoke() throws Exception;
    }

    private static <T> org.easymock.IExpectationSetters<T> expectCall(MockedMethodCall call) {
        try {
            call.invoke();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Mocked method invocation threw a checked exception", e);
        }
        return EasyMock.expectLastCall();
    }

    private void expectPreflight() {
        expectCall(preProducerCheck::run);
        expectCall(producer::initTransactions);
        expectCall(postProducerCheck::run);
        expectCall(offsetStore::start);
    }

    private void expectStartup() {
        expectCall(() -> sourceTask.initialize(EasyMock.anyObject(SourceTaskContext.class)));
        expectCall(() -> sourceTask.start(TASK_PROPS));
        expectCall(() -> statusListener.onStartup(taskId));
    }

    private void expectClose() {
        expectCall(offsetStore::stop);
        expectCall(() -> producer.close(EasyMock.anyObject(Duration.class)));
        expectCall(() -> admin.close(EasyMock.anyObject(Duration.class)));
        expectCall(transformationChain::close);
        expectCall(offsetReader::close);
    }

    private void expectTopicCreation(String topic) {
        if (config.topicCreationEnable()) {
            EasyMock.expect(admin.describeTopics(topic)).andReturn(Collections.emptyMap());
            Capture<NewTopic> newTopicCapture = EasyMock.newCapture();
            EasyMock.expect(admin.createOrFindTopics(EasyMock.capture(newTopicCapture))).andReturn(createdTopic(topic));
        }
    }
}
