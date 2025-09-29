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

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.integration.MonitorableSourceConnector;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperatorTest;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.TransactionContext;
import org.apache.kafka.connect.storage.CloseableOffsetStorageReader;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.storage.ConnectorOffsetBackingStore;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.test.util.ConcurrencyUtils;
import org.apache.kafka.connect.test.util.MockitoUtils;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.TopicAdmin;
import org.apache.kafka.connect.util.TopicCreationGroup;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.OngoingStubbing;
import org.mockito.verification.VerificationMode;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.WARN)
public class ExactlyOnceWorkerSourceTaskTest {
    private static final String TOPIC = "topic";
    private static final Map<String, byte[]> PARTITION = Collections.singletonMap("key", "partition".getBytes());
    private static final Map<String, ?> OFFSET = offset(12);

    // Connect-format data
    private static final Schema KEY_SCHEMA = Schema.INT32_SCHEMA;
    private static final Integer KEY = -1;
    private static final Schema RECORD_SCHEMA = Schema.INT64_SCHEMA;
    private static final Long VALUE_1 = 12L;
    private static final Long VALUE_2 = 13L;
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
    @Mock private ErrorHandlingMetrics errorHandlingMetrics;
    private Time time;
    private ExactlyOnceWorkerSourceTask workerTask;
    @Mock private SourceTask sourceTask;
    @Mock private Converter keyConverter;
    @Mock private Converter valueConverter;
    @Mock private HeaderConverter headerConverter;
    @Mock private TransformationChain<SourceRecord, SourceRecord> transformationChain;
    @Mock private Producer<byte[], byte[]> producer;
    @Mock private TopicAdmin admin;
    @Mock private CloseableOffsetStorageReader offsetReader;
    @Mock private OffsetStorageWriter offsetWriter;
    @Mock private ClusterConfigState clusterConfigState;
    @Mock private TaskStatus.Listener statusListener;
    @Mock private StatusBackingStore statusBackingStore;
    @Mock private ConnectorOffsetBackingStore offsetStore;
    @Mock private Runnable preProducerCheck;
    @Mock private Runnable postProducerCheck;

    private static final Map<String, String> TASK_PROPS = new HashMap<>();
    static {
        TASK_PROPS.put(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());
    }
    private static final TaskConfig TASK_CONFIG = new TaskConfig(TASK_PROPS);

    private static final SourceRecord SOURCE_RECORD_1 =
            new SourceRecord(PARTITION, OFFSET, TOPIC, null, KEY_SCHEMA, KEY, RECORD_SCHEMA, VALUE_1);
    private static final SourceRecord SOURCE_RECORD_2 =
            new SourceRecord(PARTITION, OFFSET, TOPIC, null, KEY_SCHEMA, KEY, RECORD_SCHEMA, VALUE_2);

    private static final List<SourceRecord> RECORDS = Arrays.asList(SOURCE_RECORD_1, SOURCE_RECORD_2);
    private final AtomicReference<CountDownLatch> pollLatch = new AtomicReference<>(new CountDownLatch(0));
    private final AtomicReference<List<SourceRecord>> pollRecords = new AtomicReference<>(RECORDS);

    private boolean enableTopicCreation;

    private boolean taskStarted;
    private Future<?> workerTaskFuture;


    public void setup(boolean enableTopicCreation) throws Exception {
        this.enableTopicCreation = enableTopicCreation;
        this.taskStarted = false;
        Map<String, String> workerProps = workerProps();
        plugins = new Plugins(workerProps);
        config = new StandaloneConfig(workerProps);
        sourceConfig = new SourceConnectorConfig(plugins, sourceConnectorProps(), true);
        metrics = new MockConnectMetrics();
        time = Time.SYSTEM;
        when(offsetStore.primaryOffsetsTopic()).thenReturn("offsets-topic");
        when(sourceTask.poll()).thenAnswer(invocation -> {
            // Capture the records we'll return before decrementing the poll latch,
            // since test cases may mutate the poll records field immediately
            // after awaiting the latch
            List<SourceRecord> result = pollRecords.get();
            pollLatch.get().countDown();
            Thread.sleep(10);
            return result;
        });
    }

    @AfterEach
    public void teardown() throws Exception {
        // In most tests, we don't really care about how many times the task got polled,
        // how many times we prepared to write offsets, etc.
        // We add these verify(..., anyTimes())... calls to make sure that those interactions
        // with our mocks don't cause the verifyNoMoreInteractions(...) call to fail.
        // Individual tests can add more fine-grained verification logic if necessary; the
        // verifications here will not conflict with them

        verify(sourceTask, MockitoUtils.anyTimes()).poll();
        verify(sourceTask, MockitoUtils.anyTimes()).commit();
        verify(sourceTask, MockitoUtils.anyTimes()).commitRecord(any(), any());

        verify(offsetWriter, MockitoUtils.anyTimes()).offset(PARTITION, OFFSET);
        verify(offsetWriter, MockitoUtils.anyTimes()).beginFlush();
        verify(offsetWriter, MockitoUtils.anyTimes()).doFlush(any());

        if (enableTopicCreation) {
            verify(admin, MockitoUtils.anyTimes()).describeTopics(TOPIC);
        }

        verify(statusBackingStore, MockitoUtils.anyTimes()).getTopic(any(), any());

        verify(offsetStore, MockitoUtils.anyTimes()).primaryOffsetsTopic();

        verifyNoMoreInteractions(statusListener, producer, sourceTask, admin, offsetWriter, statusBackingStore, offsetStore, preProducerCheck, postProducerCheck);
        if (metrics != null) metrics.stop();
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

    private static Map<String, ?> offset(int n) {
        return Collections.singletonMap("key", n);
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
                config, clusterConfigState, metrics, errorHandlingMetrics, plugins.delegatingLoader(), time, RetryWithToleranceOperatorTest.noneOperator(), statusBackingStore,
                sourceConfig, Runnable::run, preProducerCheck, postProducerCheck, Collections::emptyList);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRemoveMetrics(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        createWorkerTask();

        workerTask.removeMetrics();

        assertEquals(emptySet(), filterToTaskMetrics(metrics.metrics().metrics().keySet()));
    }

    private Set<MetricName> filterToTaskMetrics(Set<MetricName> metricNames) {
        return metricNames
                .stream()
                .filter(m -> metrics.registry().taskGroupName().equals(m.group())
                        || metrics.registry().sourceTaskGroupName().equals(m.group()))
                .collect(Collectors.toSet());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testStartPaused(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        createWorkerTask(TargetState.PAUSED);

        final CountDownLatch pauseLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            pauseLatch.countDown();
            return null;
        }).when(statusListener).onPause(eq(taskId));

        startTaskThread();

        assertTrue(pauseLatch.await(5, TimeUnit.SECONDS));

        awaitShutdown();

        verify(statusListener).onPause(taskId);
        verify(sourceTask, never()).start(any());
        verify(sourceTask, never()).poll();
        verifyCleanShutdown();
        assertPollMetrics(0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPause(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        createWorkerTask();

        expectSuccessfulSends();
        expectSuccessfulFlushes();

        final CountDownLatch pauseLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            pauseLatch.countDown();
            return null;
        }).when(statusListener).onPause(eq(taskId));

        startTaskThread();

        awaitPolls(2);
        workerTask.transitionTo(TargetState.PAUSED);

        long pollsBeforePause = pollCount();
        ConcurrencyUtils.awaitLatch(pauseLatch, "task did not pause in time");

        // since the transition is observed asynchronously, the count could be off by one loop iteration
        assertTrue(pollCount() - pollsBeforePause <= 1);

        awaitShutdown();

        verify(statusListener).onPause(taskId);
        // Task should have flushed offsets for every record poll, once on pause, and once for end-of-life offset commit
        verifyTransactions(pollCount() + 2, pollCount() + 2);
        verifySends();
        verifyPossibleTopicCreation();
        // make sure we didn't poll again after triggering shutdown
        assertTrue(pollCount() - pollsBeforePause <= 1);

        verifyPreflight();
        verifyStartup();
        verifyCleanShutdown();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFailureInPreProducerCheck(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        createWorkerTask();

        Exception exception = new ConnectException("Failed to perform zombie fencing");
        doThrow(exception).when(preProducerCheck).run();

        workerTask.initialize(TASK_CONFIG);
        // No need to execute on a separate thread; preflight checks should all take place before the poll-send loop starts
        workerTask.run();
        verify(preProducerCheck).run();
        verifyShutdown(true, false);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFailureInProducerInitialization(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        createWorkerTask();

        Exception exception = new ConnectException("You can't do that!");
        doThrow(exception).when(producer).initTransactions();

        workerTask.initialize(TASK_CONFIG);
        // No need to execute on a separate thread; preflight checks should all take place before the poll-send loop starts
        workerTask.run();
        verify(preProducerCheck).run();
        verify(producer).initTransactions();
        verifyShutdown(true, false);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFailureInPostProducerCheck(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        createWorkerTask();

        Exception exception = new ConnectException("New task configs for the connector have already been generated");
        doThrow(exception).when(postProducerCheck).run();

        workerTask.initialize(TASK_CONFIG);
        // No need to execute on a separate thread; preflight checks should all take place before the poll-send loop starts
        workerTask.run();
        verify(preProducerCheck).run();
        verify(producer).initTransactions();
        verify(postProducerCheck).run();
        verifyShutdown(true, false);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFailureInOffsetStoreStart(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        createWorkerTask();

        Exception exception = new ConnectException("No soup for you!");
        doThrow(exception).when(offsetStore).start();

        workerTask.initialize(TASK_CONFIG);
        // No need to execute on a separate thread; preflight checks should all take place before the poll-send loop starts
        workerTask.run();
        verify(preProducerCheck).run();
        verify(producer).initTransactions();
        verify(postProducerCheck).run();
        verify(offsetStore).start();
        verifyShutdown(true, false);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPollsInBackground(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        createWorkerTask();

        expectSuccessfulSends();
        expectSuccessfulFlushes();

        startTaskThread();

        int minPolls = 10;
        awaitPolls(minPolls);
        awaitShutdown();

        // Task should have flushed offsets for every record poll and for end-of-life offset commit
        verifyTransactions(pollCount() + 1, pollCount() + 1);
        verifySends();
        verifyPossibleTopicCreation();

        verifyPreflight();
        verifyStartup();
        verifyCleanShutdown();
        assertPollMetrics(minPolls);
        assertTransactionMetrics(RECORDS.size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFailureInPoll(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        createWorkerTask();

        final CountDownLatch pollLatch = new CountDownLatch(1);
        final RuntimeException exception = new RuntimeException();
        when(sourceTask.poll()).thenAnswer(invocation -> {
            pollLatch.countDown();
            throw exception;
        });

        startTaskThread();

        ConcurrencyUtils.awaitLatch(pollLatch, "task was not polled in time");
        //Failure in poll should trigger automatic stop of the worker
        awaitShutdown(false);

        verifyPreflight();
        verifyStartup();
        verifyShutdown(true, false);
        assertPollMetrics(0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFailureInPollAfterCancel(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        createWorkerTask();

        final CountDownLatch pollLatch = new CountDownLatch(1);
        final CountDownLatch workerCancelLatch = new CountDownLatch(1);
        final RuntimeException exception = new RuntimeException();
        when(sourceTask.poll()).thenAnswer(invocation -> {
            pollLatch.countDown();
            ConcurrencyUtils.awaitLatch(workerCancelLatch, "task was not cancelled in time");
            throw exception;
        });

        startTaskThread();

        ConcurrencyUtils.awaitLatch(pollLatch, "task was not polled in time");
        workerTask.cancel();
        workerCancelLatch.countDown();
        awaitShutdown(false);

        verifyPreflight();
        verifyStartup();
        verifyShutdown(false, true);
        assertPollMetrics(0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFailureInPollAfterStop(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        createWorkerTask();

        final CountDownLatch pollLatch = new CountDownLatch(1);
        final CountDownLatch workerStopLatch = new CountDownLatch(1);
        final RuntimeException exception = new RuntimeException();
        // We check if there are offsets that need to be committed before shutting down the task
        when(sourceTask.poll()).thenAnswer(invocation -> {
            pollLatch.countDown();
            ConcurrencyUtils.awaitLatch(workerStopLatch, "task was not stopped in time");
            throw exception;
        });

        startTaskThread();

        ConcurrencyUtils.awaitLatch(pollLatch, "task was not polled in time");
        workerTask.stop();
        workerStopLatch.countDown();
        awaitShutdown(false);

        verifyTransactions(0, 0);

        verifyPreflight();
        verifyStartup();
        verifyCleanShutdown();
        assertPollMetrics(0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPollReturnsNoRecords(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        // Test that the task handles an empty list of records
        createWorkerTask();

        // Make sure the task returns empty batches from poll before we start polling it
        pollRecords.set(Collections.emptyList());

        when(offsetWriter.beginFlush()).thenReturn(false);

        startTaskThread();

        awaitEmptyPolls(10);

        awaitShutdown();

        // task commits for each empty poll, plus the final commit
        verifyTransactions(pollCount() + 1, 0);

        verifyPreflight();
        verifyStartup();
        verifyCleanShutdown();
        assertPollMetrics(0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPollBasedCommit(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        Map<String, String> connectorProps = sourceConnectorProps(SourceTask.TransactionBoundary.POLL);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);

        createWorkerTask();

        expectSuccessfulSends();
        expectSuccessfulFlushes();

        startTaskThread();

        awaitPolls(10);

        awaitShutdown();

        // Task should have flushed offsets for every record poll, and for end-of-life offset commit
        verifyTransactions(pollCount() + 1, pollCount() + 1);
        verifySends();
        verifyPossibleTopicCreation();

        verifyPreflight();
        verifyStartup();
        verifyCleanShutdown();
        assertPollMetrics(1);
        assertTransactionMetrics(RECORDS.size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testIntervalBasedCommit(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        long commitInterval = 618;
        Map<String, String> connectorProps = sourceConnectorProps(SourceTask.TransactionBoundary.INTERVAL);
        connectorProps.put(TRANSACTION_BOUNDARY_INTERVAL_CONFIG, Long.toString(commitInterval));
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);

        time = new MockTime();

        createWorkerTask();

        expectSuccessfulSends();
        expectSuccessfulFlushes();

        startTaskThread();

        awaitPolls(2);
        assertEquals(0, flushCount(), "No flushes should have taken place before offset commit interval has elapsed");
        time.sleep(commitInterval);

        awaitPolls(2);
        assertEquals(1, flushCount(), "One flush should have taken place after offset commit interval has elapsed");
        time.sleep(commitInterval * 2);

        awaitPolls(2);
        assertEquals(2, flushCount(), 
                "Two flushes should have taken place after offset commit interval has elapsed again");

        awaitShutdown();

        // Task should have flushed offsets twice based on offset commit interval, and performed final end-of-life offset commit
        verifyTransactions(3, 3);
        verifySends();
        verifyPossibleTopicCreation();

        verifyPreflight();
        verifyStartup();
        verifyCleanShutdown();
        assertPollMetrics(2);
        assertTransactionMetrics(RECORDS.size() * 2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testConnectorCommitOnBatch(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        testConnectorBasedCommit(TransactionContext::commitTransaction, false);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testConnectorCommitOnRecord(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        testConnectorBasedCommit(ctx -> ctx.commitTransaction(SOURCE_RECORD_2), false);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testConnectorAbortOnBatch(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        testConnectorBasedCommit(TransactionContext::abortTransaction, true);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testConnectorAbortOnRecord(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        testConnectorBasedCommit(ctx -> ctx.abortTransaction(SOURCE_RECORD_2), true);
    }

    private void testConnectorBasedCommit(Consumer<TransactionContext> requestCommit, boolean abort) throws Exception {
        setup(enableTopicCreation);
        Map<String, String> connectorProps = sourceConnectorProps(SourceTask.TransactionBoundary.CONNECTOR);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        createWorkerTask();

        expectSuccessfulSends();
        expectSuccessfulFlushes();

        TransactionContext transactionContext = workerTask.sourceTaskContext.transactionContext();

        startTaskThread();

        awaitPolls(3);
        assertEquals(0, flushCount(),
                "No flushes should have taken place without connector requesting transaction commit");

        requestCommit.accept(transactionContext);
        awaitPolls(3);
        assertEquals(1, flushCount(),
                "One flush should have taken place after transaction commit/abort was requested");

        awaitPolls(3);
        assertEquals(1, flushCount(),
                "Only one flush should still have taken place without connector re-requesting commit/abort, even on identical records");

        awaitShutdown();

        assertEquals(1, flushCount(),
                "Task should have flushed offsets once based on connector-defined boundaries, and skipped final end-of-life offset commit");
        // We begin a new transaction after connector-requested aborts so that we can still write offsets for the source records that were aborted
        verify(producer, times(abort ? 3 : 2)).beginTransaction();
        verifySends();
        if (abort) {
            verify(producer).abortTransaction();
        }
        verify(producer).commitTransaction();

        verifyPreflight();
        verifyStartup();
        verifyCleanShutdown();
        verifyPossibleTopicCreation();
        assertPollMetrics(1);
        assertTransactionMetrics(abort ? 0 : (3 * RECORDS.size()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testConnectorAbortsEmptyTransaction(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        Map<String, String> connectorProps = sourceConnectorProps(SourceTask.TransactionBoundary.CONNECTOR);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        createWorkerTask();

        expectPossibleTopicCreation();
        expectTaskGetTopic();
        expectApplyTransformationChain();
        expectConvertHeadersAndKeyValue();
        TransactionContext transactionContext = workerTask.sourceTaskContext.transactionContext();

        startTaskThread();

        // Ensure the task produces at least one non-empty batch
        awaitPolls(1);
        // Start returning empty batches from SourceTask::poll
        awaitEmptyPolls(1);
        // The task commits the active transaction, which should not be empty and should
        // result in a single call to Producer::commitTransaction
        transactionContext.commitTransaction();

        // Ensure the task produces at least one empty batch after the transaction has been committed
        awaitEmptyPolls(1);
        // The task aborts the active transaction, which has no records in it and should not trigger a call to
        // Producer::abortTransaction
        transactionContext.abortTransaction();
        // Make sure we go through at least one more poll, to give the framework a chance to handle the
        // transaction abort request
        awaitEmptyPolls(1);

        awaitShutdown(true);

        verify(producer, times(1)).beginTransaction();
        verify(producer, times(1)).commitTransaction();
        verify(producer, atLeast(RECORDS.size())).send(any(), any());
        // We never abort transactions in this test!
        verify(producer, never()).abortTransaction();

        verifyPreflight();
        verifyStartup();
        verifyCleanShutdown();
        verifyPossibleTopicCreation();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMixedConnectorTransactionBoundaryCommitLastRecordAbortBatch(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        // We fail tasks that try to abort and commit a transaction for the same record or same batch
        // But we don't fail if they try to commit the last record of a batch and abort the entire batch
        // Instead, we give precedence to the record-based operation

        Map<String, String> connectorProps = sourceConnectorProps(SourceTask.TransactionBoundary.CONNECTOR);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        createWorkerTask();

        expectPossibleTopicCreation();
        expectTaskGetTopic();
        expectApplyTransformationChain();
        expectConvertHeadersAndKeyValue();
        when(offsetWriter.beginFlush())
                .thenReturn(true)
                .thenReturn(false);

        workerTask.initialize(TASK_CONFIG);

        TransactionContext transactionContext = workerTask.sourceTaskContext.transactionContext();

        // Request that the batch be aborted
        transactionContext.abortTransaction();
        // Request that the last record in the batch be committed
        transactionContext.commitTransaction(RECORDS.get(RECORDS.size() - 1));

        workerTask.toSend = RECORDS;
        assertTrue(workerTask.sendRecords());

        verifyTransactions(2, 1);
        verifySends(RECORDS.size());
        // We never abort transactions in this test!
        verify(producer, never()).abortTransaction();

        verifyPossibleTopicCreation();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMixedConnectorTransactionBoundaryAbortLastRecordCommitBatch(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        // We fail tasks that try to abort and commit a transaction for the same record or same batch
        // But we don't fail if they try to abort the last record of a batch and commit the entire batch
        // Instead, we give precedence to the record-based operation

        Map<String, String> connectorProps = sourceConnectorProps(SourceTask.TransactionBoundary.CONNECTOR);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        createWorkerTask();

        expectPossibleTopicCreation();
        expectTaskGetTopic();
        expectApplyTransformationChain();
        expectConvertHeadersAndKeyValue();
        when(offsetWriter.beginFlush())
                .thenReturn(true)
                .thenReturn(false);

        workerTask.initialize(TASK_CONFIG);

        TransactionContext transactionContext = workerTask.sourceTaskContext.transactionContext();

        // Request that the last record in the batch be aborted
        transactionContext.abortTransaction(RECORDS.get(RECORDS.size() - 1));
        // Request that the batch be committed
        transactionContext.commitTransaction();

        workerTask.toSend = RECORDS;
        assertTrue(workerTask.sendRecords());

        verify(offsetWriter, times(2)).beginFlush();
        verify(sourceTask, times(2)).commit();
        // We open a transaction once for the aborted batch, and once to commit offsets for that batch
        verify(producer, times(2)).beginTransaction();
        verify(producer, times(1)).commitTransaction();
        verify(offsetWriter, times(1)).doFlush(any());
        verify(producer, times(1)).abortTransaction();

        verifySends(RECORDS.size());

        verifyPossibleTopicCreation();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCommitFlushSyncCallbackFailure(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        Exception failure = new RecordTooLargeException();
        when(offsetWriter.beginFlush()).thenReturn(true);
        when(offsetWriter.doFlush(any())).thenAnswer(invocation -> {
            Callback<Void> callback = invocation.getArgument(0);
            callback.onCompletion(failure, null);
            return null;
        });
        testCommitFailure(failure, false);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCommitFlushAsyncCallbackFailure(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        Exception failure = new RecordTooLargeException();
        when(offsetWriter.beginFlush()).thenReturn(true);
        // doFlush delegates its callback to the producer,
        // which delays completing the callback until commitTransaction
        AtomicReference<Callback<Void>> callback = new AtomicReference<>();
        when(offsetWriter.doFlush(any())).thenAnswer(invocation -> {
            callback.set(invocation.getArgument(0));
            return null;
        });
        doAnswer(invocation -> {
            callback.get().onCompletion(failure, null);
            return null;
        }).when(producer).commitTransaction();
        testCommitFailure(failure, true);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCommitTransactionFailure(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        Exception failure = new RecordTooLargeException();
        when(offsetWriter.beginFlush()).thenReturn(true);
        doThrow(failure).when(producer).commitTransaction();
        testCommitFailure(failure, true);
    }

    private void testCommitFailure(Exception commitException, boolean executeCommit) throws Exception {
        createWorkerTask();

        // Unlike the standard WorkerSourceTask class, this one fails permanently when offset commits don't succeed
        final CountDownLatch taskFailure = new CountDownLatch(1);
        final AtomicReference<Exception> taskException = new AtomicReference<>();
        doAnswer(invocation -> {
            taskFailure.countDown();
            taskException.set(invocation.getArgument(1));
            return null;
        }).when(statusListener).onFailure(eq(taskId), any());

        expectSuccessfulSends();

        startTaskThread();

        ConcurrencyUtils.awaitLatch(taskFailure, "task did not fail in time");

        awaitShutdown();

        assertEquals(commitException, taskException.get().getCause());
        verifySends();
        verifyPossibleTopicCreation();
        verify(producer).beginTransaction();
        if (executeCommit) {
            verify(producer).commitTransaction();
        }
        verify(offsetWriter).cancelFlush();

        verifyPreflight();
        verifyStartup();
        verifyShutdown(true, false);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSendRecordsRetries(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        createWorkerTask();

        // Differentiate only by Kafka partition so we can reuse conversion expectations
        SourceRecord record1 = new SourceRecord(PARTITION, offset(1), TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, VALUE_1);
        SourceRecord record2 = new SourceRecord(PARTITION, offset(2), TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, VALUE_1);
        SourceRecord record3 = new SourceRecord(PARTITION, offset(3), TOPIC, 3, KEY_SCHEMA, KEY, RECORD_SCHEMA, VALUE_1);

        expectPossibleTopicCreation();
        expectTaskGetTopic();
        expectApplyTransformationChain();
        expectConvertHeadersAndKeyValue();

        // We're trying to send three records
        workerTask.toSend = Arrays.asList(record1, record2, record3);
        OngoingStubbing<Future<RecordMetadata>> producerSend = when(producer.send(any(), any()));
        // The first one is sent successfully
        producerSend = expectSuccessfulSend(producerSend);
        // The second one encounters a retriable failure
        producerSend = expectSynchronousFailedSend(producerSend, new org.apache.kafka.common.errors.TimeoutException("retriable sync failure"));
        // All subsequent sends (which should include a retry of the second record and an initial try for the third) succeed
        expectSuccessfulSend(producerSend);

        assertFalse(workerTask.sendRecords());
        assertEquals(Arrays.asList(record2, record3), workerTask.toSend);
        verify(producer).beginTransaction();
        // When using poll-based transaction boundaries, we do not commit transactions while retrying delivery for a batch
        verify(producer, never()).commitTransaction();
        verifySends(2);
        verifyPossibleTopicCreation();

        // Next they all succeed
        assertTrue(workerTask.sendRecords());
        assertNull(workerTask.toSend);
        // After the entire batch is successfully dispatched to the producer, we can finally commit the transaction
        verify(producer, times(1)).commitTransaction();
        verifySends(4);

        verify(offsetWriter).offset(PARTITION, offset(1));
        verify(offsetWriter).offset(PARTITION, offset(2));
        verify(offsetWriter).offset(PARTITION, offset(3));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSendRecordsProducerSendFailsImmediately(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, VALUE_1);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, VALUE_1);

        expectPossibleTopicCreation();
        expectConvertHeadersAndKeyValue();
        expectApplyTransformationChain();

        when(producer.send(any(), any()))
                .thenThrow(new KafkaException("Producer closed while send in progress", new InvalidTopicException(TOPIC)));

        workerTask.toSend = Arrays.asList(record1, record2);
        assertThrows(ConnectException.class, workerTask::sendRecords);

        verify(producer).beginTransaction();
        verify(producer).send(any(), any());
        verify(offsetWriter, never()).offset(any(), any());
        verifyPossibleTopicCreation();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSlowTaskStart(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        final CountDownLatch startupLatch = new CountDownLatch(1);
        final CountDownLatch finishStartupLatch = new CountDownLatch(1);

        createWorkerTask();

        doAnswer(invocation -> {
            startupLatch.countDown();
            ConcurrencyUtils.awaitLatch(finishStartupLatch, "task was not allowed to finish startup in time");
            return null;
        }).when(sourceTask).start(eq(TASK_PROPS));

        when(offsetWriter.beginFlush()).thenReturn(false);
        startTaskThread();

        // Stopping immediately while the other thread has work to do should result in no polling, no offset commits,
        // exiting the work thread immediately, and the stop() method will be invoked in the background thread since it
        // cannot be invoked immediately in the thread trying to stop the task.
        ConcurrencyUtils.awaitLatch(startupLatch, "task did not start up in time");
        workerTask.stop();
        finishStartupLatch.countDown();
        awaitShutdown(false);

        verify(sourceTask, never()).poll();
        // task commit called on shutdown
        verifyTransactions(1, 0);
        verifySends(0);

        verifyPreflight();
        verifyStartup();
        verifyCleanShutdown();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCancel(boolean enableTopicCreation) throws Exception {
        setup(enableTopicCreation);
        createWorkerTask();

        // workerTask said something dumb on twitter
        workerTask.cancel();

        verify(offsetReader).close();
        verify(producer).close(Duration.ZERO);
    }

    /**
     * @return how many times the source task was {@link SourceTask#poll() polled}
     */
    private int pollCount() {
        return (int) MockitoUtils.countInvocations(sourceTask, "poll");
    }

    /**
     * @return how many times an {@link OffsetStorageWriter#doFlush(Callback) offset flush} took place
     */
    private int flushCount() {
        return (int) MockitoUtils.countInvocations(offsetWriter, "doFlush", Callback.class);
    }

    private void awaitPolls(int minimum, List<SourceRecord> records) {
        pollRecords.set(records);
        pollLatch.set(new CountDownLatch(minimum));
        ConcurrencyUtils.awaitLatch(pollLatch.get(), "task was not polled " + minimum + " time(s) quickly enough");
    }

    private void awaitEmptyPolls(int minimum) {
        awaitPolls(minimum, Collections.emptyList());
    }

    private void awaitPolls(int minimum) {
        awaitPolls(minimum, RECORDS);
    }

    private void awaitShutdown() throws InterruptedException, ExecutionException {
        awaitShutdown(true);
    }

    private void awaitShutdown(boolean triggerStop) throws InterruptedException, ExecutionException {
        if (triggerStop)
            workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));
        workerTaskFuture.get();
    }

    private void startTaskThread() {
        workerTask.initialize(TASK_CONFIG);
        workerTaskFuture = executor.submit(workerTask);
    }

    private void expectSuccessfulFlushes() {
        when(offsetWriter.beginFlush()).thenReturn(true);
        when(offsetWriter.doFlush(any())).thenAnswer(invocation -> {
            Callback<Void> cb = invocation.getArgument(0);
            cb.onCompletion(null, null);
            return null;
        });
    }

    private void expectSuccessfulSends() {
        expectConvertHeadersAndKeyValue();
        expectApplyTransformationChain();
        expectSuccessfulSend(when(producer.send(any(), any())));
        expectTaskGetTopic();
        expectPossibleTopicCreation();
    }

    private OngoingStubbing<Future<RecordMetadata>> expectSuccessfulSend(OngoingStubbing<Future<RecordMetadata>> whenSend) {
        return whenSend.thenAnswer(invocation -> {
            org.apache.kafka.clients.producer.Callback cb = invocation.getArgument(1);
            cb.onCompletion(
                    new RecordMetadata(
                            new TopicPartition(TOPIC, 0),
                            0,
                            0,
                            0L,
                            0,
                            0),
                    null);
            return null;
        });
    }

    private OngoingStubbing<Future<RecordMetadata>> expectSynchronousFailedSend(OngoingStubbing<Future<RecordMetadata>> whenSend, Exception failure) {
        return whenSend.thenThrow(failure);
    }

    private void expectConvertHeadersAndKeyValue() {
        Headers headers = new RecordHeaders();
        for (Header header : headers) {
            when(headerConverter.fromConnectHeader(eq(TOPIC), eq(header.key()), eq(Schema.STRING_SCHEMA), eq(new String(header.value()))))
                    .thenReturn(header.value());
        }
        when(keyConverter.fromConnectData(eq(TOPIC), eq(headers), eq(KEY_SCHEMA), eq(KEY)))
                .thenReturn(SERIALIZED_KEY);
        when(valueConverter.fromConnectData(eq(TOPIC), eq(headers), eq(RECORD_SCHEMA), eq(VALUE_1)))
                .thenReturn(SERIALIZED_RECORD);
    }

    private void expectApplyTransformationChain() {
        when(transformationChain.apply(any(), any()))
                .thenAnswer(invocation -> invocation.getArgument(1));
    }

    private void expectTaskGetTopic() {
        when(statusBackingStore.getTopic(any(), eq(TOPIC)))
                .thenAnswer(invocation -> new TopicStatus(
                        invocation.getArgument(1),
                        new ConnectorTaskId(invocation.getArgument(0), 0),
                        time.milliseconds()
                ));
    }

    private void expectPossibleTopicCreation() {
        if (config.topicCreationEnable()) {
            Set<String> created = Collections.singleton(TOPIC);
            Set<String> existing = Collections.emptySet();
            TopicAdmin.TopicCreationResponse creationResponse = new TopicAdmin.TopicCreationResponse(created, existing);
            when(admin.createOrFindTopics(any())).thenReturn(creationResponse);
        }
    }

    private void verifyPreflight() {
        verify(preProducerCheck).run();
        verify(producer).initTransactions();
        verify(postProducerCheck).run();
        verify(offsetStore).start();
    }

    private void verifyStartup() {
        verify(sourceTask).initialize(any());
        verify(sourceTask).start(TASK_PROPS);
        taskStarted = true;
        verify(statusListener).onStartup(taskId);
    }

    private void verifyCleanShutdown() throws Exception {
        verifyShutdown(false, false);
    }

    private void verifyShutdown(boolean taskFailed, boolean cancelled) throws Exception {
        if (cancelled) {
            verify(producer).close(Duration.ZERO);
            verify(producer, times(2)).close(any());
            verify(offsetReader, times(2)).close();
        } else {
            verify(producer).close(any());
            verify(offsetReader).close();
        }

        verify(offsetStore).stop();
        verify(admin).close(any());
        verify(transformationChain).close();
        verify(headerConverter).close();

        if (taskStarted) {
            verify(sourceTask).stop();
        }

        if (taskFailed) {
            verify(statusListener).onFailure(eq(taskId), any());
        } else if (!cancelled) {
            verify(statusListener).onShutdown(taskId);
        }
    }

    private void verifyPossibleTopicCreation() {
        if (enableTopicCreation) {
            verify(admin).createOrFindTopics(any());
        } else {
            verify(admin, never()).createOrFindTopics(any());
        }
    }

    private void verifySends() {
        verifySends(pollCount() * RECORDS.size());
    }

    private void verifySends(int count) {
        verify(producer, times(count)).send(any(), any());
    }

    private void verifyTransactions(int numTaskCommits, int numProducerCommits) throws InterruptedException {
        // these operations happen on every commit opportunity
        VerificationMode commitOpportunities = times(numTaskCommits);
        verify(offsetWriter, commitOpportunities).beginFlush();
        verify(sourceTask, commitOpportunities).commit();
        // these operations only happen on non-empty commits
        VerificationMode commits = times(numProducerCommits);
        verify(producer, commits).beginTransaction();
        verify(producer, commits).commitTransaction();
        verify(offsetWriter, commits).doFlush(any());
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
            assertTrue(actualAvg > actualMin, "Average transaction size should be greater than minimum transaction size");
            assertTrue(actualAvg < actualMax, "Average transaction size should be less than maximum transaction size");
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

    private abstract static class TestSourceTask extends SourceTask {
    }

}
