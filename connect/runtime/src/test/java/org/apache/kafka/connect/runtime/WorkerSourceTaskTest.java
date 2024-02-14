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
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.TopicPartition;
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
import org.apache.kafka.connect.test.util.ConcurrencyUtils;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.ParameterizedTest;
import org.apache.kafka.connect.util.TopicAdmin;
import org.apache.kafka.connect.util.TopicCreationGroup;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.AdditionalAnswers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
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
import java.util.function.Supplier;

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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked"})
@RunWith(Parameterized.class)
public class WorkerSourceTaskTest {

    public static final String POLL_TIMEOUT_MSG = "Timeout waiting for poll";
    @org.junit.Rule
    public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

    private static final String TOPIC = "topic";
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

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ConnectorTaskId taskId = new ConnectorTaskId("job", 0);
    private WorkerConfig config;
    private SourceConnectorConfig sourceConfig;
    private Plugins plugins;
    private MockConnectMetrics metrics;
    @Mock
    private SourceTask sourceTask;
    @Mock
    private Converter keyConverter;
    @Mock
    private Converter valueConverter;
    @Mock
    private HeaderConverter headerConverter;
    @Mock
    private TransformationChain<SourceRecord, SourceRecord> transformationChain;
    @Mock
    private KafkaProducer<byte[], byte[]> producer;
    @Mock
    private TopicAdmin admin;
    @Mock
    private CloseableOffsetStorageReader offsetReader;
    @Mock
    private OffsetStorageWriter offsetWriter;
    @Mock
    private ConnectorOffsetBackingStore offsetStore;
    @Mock
    private ClusterConfigState clusterConfigState;
    private WorkerSourceTask workerTask;
    @Mock
    private TaskStatus.Listener statusListener;
    @Mock
    private StatusBackingStore statusBackingStore;
    @Mock
    private ErrorHandlingMetrics errorHandlingMetrics;

    private static final Map<String, String> TASK_PROPS = new HashMap<>();

    static {
        TASK_PROPS.put(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());
    }

    private static final TaskConfig TASK_CONFIG = new TaskConfig(TASK_PROPS);

    private static final List<SourceRecord> RECORDS = Collections.singletonList(
            new SourceRecord(PARTITION, OFFSET, "topic", null, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD)
    );

    private final boolean enableTopicCreation;

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
        verifyNoMoreInteractions(statusListener);
    }

    private void createWorkerTask() {
        createWorkerTask(TargetState.STARTED, RetryWithToleranceOperatorTest.noopOperator());
    }

    private void createWorkerTaskWithErrorToleration() {
        createWorkerTask(TargetState.STARTED, RetryWithToleranceOperatorTest.allOperator());
    }

    private void createWorkerTask(TargetState initialState) {
        createWorkerTask(initialState, RetryWithToleranceOperatorTest.noopOperator());
    }

    private void createWorkerTask(TargetState initialState, RetryWithToleranceOperator<SourceRecord> retryWithToleranceOperator) {
        createWorkerTask(initialState, keyConverter, valueConverter, headerConverter, retryWithToleranceOperator);
    }

    private void createWorkerTask(TargetState initialState, Converter keyConverter, Converter valueConverter,
                                  HeaderConverter headerConverter, RetryWithToleranceOperator<SourceRecord> retryWithToleranceOperator) {
        workerTask = new WorkerSourceTask(taskId, sourceTask, statusListener, initialState, keyConverter, valueConverter, errorHandlingMetrics, headerConverter,
                transformationChain, producer, admin, TopicCreationGroup.configuredGroups(sourceConfig),
                offsetReader, offsetWriter, offsetStore, config, clusterConfigState, metrics, plugins.delegatingLoader(), Time.SYSTEM,
                retryWithToleranceOperator, statusBackingStore, Runnable::run, Collections::emptyList);
    }

    @Test
    public void testStartPaused() throws Exception {
        final CountDownLatch pauseLatch = new CountDownLatch(1);

        createWorkerTask(TargetState.PAUSED);
        doAnswer(invocation -> {
            pauseLatch.countDown();
            return null;
        }).when(statusListener).onPause(taskId);

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        assertTrue(pauseLatch.await(5, TimeUnit.SECONDS));
        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();

        verify(statusListener).onPause(taskId);
        verify(statusListener).onShutdown(taskId);
        verifyClose();
    }

    @Test
    public void testPause() throws Exception {
        createWorkerTask();

        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch pollLatch = expectPolls(10, count);
        // In this test, we don't flush, so nothing goes any further than the offset writer

        expectTopicCreation(TOPIC);
        expectOffsetFlush();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);
        ConcurrencyUtils.awaitLatch(pollLatch, POLL_TIMEOUT_MSG);

        workerTask.transitionTo(TargetState.PAUSED);

        int priorCount = count.get();
        Thread.sleep(100);

        // since the transition is observed asynchronously, the count could be off by one loop iteration
        assertTrue(count.get() - priorCount <= 1);

        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();

        verifyCleanStartup();
        verifyTaskGetTopic(count.get());
        verifyOffsetFlush(true);
        verifyTopicCreation(TOPIC);
        verify(statusListener).onPause(taskId);
        verify(statusListener).onShutdown(taskId);
        verify(sourceTask).stop();
        verify(offsetWriter).offset(PARTITION, OFFSET);
        verifyClose();
    }

    @Test
    public void testPollsInBackground() throws Exception {
        createWorkerTask();

        final CountDownLatch pollLatch = expectPolls(10);

        expectTopicCreation(TOPIC);
        expectOffsetFlush();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        ConcurrencyUtils.awaitLatch(pollLatch, POLL_TIMEOUT_MSG);
        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();
        assertPollMetrics(10);
        verifyCleanStartup();
        verifyOffsetFlush(true);
        verify(offsetWriter).offset(PARTITION, OFFSET);
        verify(statusListener).onShutdown(taskId);
        verifyClose();
    }

    @Test
    public void testFailureInPoll() throws Exception {
        createWorkerTask();

        final CountDownLatch pollLatch = new CountDownLatch(1);
        final RuntimeException exception = new RuntimeException();
        when(sourceTask.poll()).thenAnswer(invocation -> {
            pollLatch.countDown();
            throw exception;
        });

        expectEmptyOffsetFlush();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        ConcurrencyUtils.awaitLatch(pollLatch, POLL_TIMEOUT_MSG);
        //Failure in poll should trigger automatic stop of the task
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();
        assertPollMetrics(0);

        verifyCleanStartup();
        verify(statusListener).onFailure(taskId, exception);
        verify(sourceTask).stop();
        assertShouldSkipCommit();
        verifyOffsetFlush(true);
        verifyClose();
    }

    @Test
    public void testFailureInPollAfterCancel() throws Exception {
        createWorkerTask();

        final CountDownLatch pollLatch = new CountDownLatch(1);
        final CountDownLatch workerCancelLatch = new CountDownLatch(1);
        final RuntimeException exception = new RuntimeException();
        when(sourceTask.poll()).thenAnswer(invocation -> {
            pollLatch.countDown();
            ConcurrencyUtils.awaitLatch(workerCancelLatch, "Timeout waiting for main test thread to cancel task.");
            throw exception;
        });

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        ConcurrencyUtils.awaitLatch(pollLatch, POLL_TIMEOUT_MSG);
        workerTask.cancel();
        workerCancelLatch.countDown();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();
        assertPollMetrics(0);

        verifyCleanStartup();
        verify(offsetReader, atLeastOnce()).close();
        verify(producer).close(Duration.ZERO);
        verify(sourceTask).stop();
        verify(admin).close(any(Duration.class));
        verify(transformationChain).close();
        verify(offsetStore).stop();

        try {
            verify(headerConverter).close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testFailureInPollAfterStop() throws Exception {
        createWorkerTask();

        final CountDownLatch pollLatch = new CountDownLatch(1);
        final CountDownLatch workerStopLatch = new CountDownLatch(1);
        final RuntimeException exception = new RuntimeException();
        when(sourceTask.poll()).thenAnswer(invocation -> {
            pollLatch.countDown();
            ConcurrencyUtils.awaitLatch(workerStopLatch, "Timeout waiting for main test thread to stop task");
            throw exception;
        });
        expectOffsetFlush();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        ConcurrencyUtils.awaitLatch(pollLatch, POLL_TIMEOUT_MSG);
        workerTask.stop();
        workerStopLatch.countDown();
        assertTrue(workerTask.awaitStop(1000));
        assertShouldSkipCommit();

        taskFuture.get();
        assertPollMetrics(0);

        verifyCleanStartup();
        verify(statusListener).onShutdown(taskId);
        verify(sourceTask).stop();
        verifyOffsetFlush(true);
        verifyClose();
    }

    @Test
    public void testPollReturnsNoRecords() throws Exception {
        // Test that the task handles an empty list of records
        createWorkerTask();

        // We'll wait for some data, then trigger a flush
        final CountDownLatch pollLatch = expectEmptyPolls(new AtomicInteger());
        expectEmptyOffsetFlush();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        ConcurrencyUtils.awaitLatch(pollLatch, POLL_TIMEOUT_MSG);
        assertTrue(workerTask.commitOffsets());
        verify(offsetWriter).beginFlush(anyLong(), any(TimeUnit.class));

        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));
        verify(offsetWriter, times(2)).beginFlush(anyLong(), any(TimeUnit.class));
        verifyNoMoreInteractions(offsetWriter);

        taskFuture.get();
        assertPollMetrics(0);

        verifyCleanStartup();
        verify(sourceTask).stop();
        verify(statusListener).onShutdown(taskId);
        verifyClose();
    }

    @Test
    public void testCommit() throws Exception {
        // Test that the task commits properly when prompted
        createWorkerTask();

        // We'll wait for some data, then trigger a flush
        final CountDownLatch pollLatch = expectPolls(1);

        expectTopicCreation(TOPIC);
        expectBeginFlush(Arrays.asList(true, false).iterator()::next);
        expectOffsetFlush(true, true);

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        ConcurrencyUtils.awaitLatch(pollLatch, POLL_TIMEOUT_MSG);
        assertTrue(workerTask.commitOffsets());

        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();
        assertPollMetrics(1);

        verifyCleanStartup();
        verifyTopicCreation(TOPIC);
        verify(offsetWriter, times(2)).beginFlush(anyLong(), any(TimeUnit.class));
        verify(offsetWriter, atLeastOnce()).offset(PARTITION, OFFSET);
        verify(sourceTask).stop();
        verify(statusListener).onShutdown(taskId);
        verifyOffsetFlush(true, 2);
        verifyClose();
    }

    @Test
    public void testCommitFailure() throws Exception {
        // Test that the task commits properly when prompted
        createWorkerTask();

        // We'll wait for some data, then trigger a flush
        final CountDownLatch pollLatch = expectPolls(1);
        expectBeginFlush();
        expectOffsetFlush(true, false);

        expectTopicCreation(TOPIC);

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        ConcurrencyUtils.awaitLatch(pollLatch, POLL_TIMEOUT_MSG);
        assertTrue(workerTask.commitOffsets());

        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();
        assertPollMetrics(1);

        verifyCleanStartup();
        verify(sourceTask).stop();
        verify(offsetWriter, atLeastOnce()).offset(PARTITION, OFFSET);
        verify(statusListener).onShutdown(taskId);

        verifyOffsetFlush(true); // First call to doFlush() succeeded
        verifyOffsetFlush(false); // Second call threw a TimeoutException
        verifyClose();
    }

    @Test
    public void testSendRecordsRetries() {
        createWorkerTask();

        // Differentiate only by Kafka partition, so we can reuse conversion expectations
        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, "topic", 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, "topic", 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record3 = new SourceRecord(PARTITION, OFFSET, "topic", 3, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectTopicCreation(TOPIC);
        expectPreliminaryCalls();

        expectTaskGetTopic();

        when(producer.send(any(ProducerRecord.class), any(Callback.class)))
                // First round
                .thenAnswer(producerSendAnswer(true))
                // Any Producer retriable exception should work here
                .thenThrow(new org.apache.kafka.common.errors.TimeoutException("retriable sync failure"))
                // Second round
                .thenAnswer(producerSendAnswer(true))
                .thenAnswer(producerSendAnswer(true));

        // Try to send 3, make first pass, second fail. Should save last two
        workerTask.toSend = Arrays.asList(record1, record2, record3);
        workerTask.sendRecords();
        assertEquals(Arrays.asList(record2, record3), workerTask.toSend);

        // Next they all succeed
        workerTask.sendRecords();
        assertNull(workerTask.toSend);
    }

    @Test
    public void testSendRecordsProducerCallbackFail() {
        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, "topic", 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, "topic", 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectTopicCreation(TOPIC);

        expectSendRecordProducerCallbackFail();

        workerTask.toSend = Arrays.asList(record1, record2);
        assertThrows(ConnectException.class, () -> workerTask.sendRecords());

        verify(transformationChain, times(2)).apply(any(), any(SourceRecord.class));
        verify(keyConverter, times(2)).fromConnectData(anyString(), any(Headers.class), eq(KEY_SCHEMA), eq(KEY));
        verify(valueConverter, times(2)).fromConnectData(anyString(), any(Headers.class), eq(RECORD_SCHEMA), eq(RECORD));
    }

    @Test
    public void testSendRecordsProducerSendFailsImmediately() {
        createWorkerTask();

        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectPreliminaryCalls();
        expectTopicCreation(TOPIC);

        when(producer.send(any(ProducerRecord.class), any(Callback.class)))
                .thenThrow(new KafkaException("Producer closed while send in progress", new InvalidTopicException(TOPIC)));

        workerTask.toSend = Arrays.asList(record1, record2);
        assertThrows(ConnectException.class, () -> workerTask.sendRecords());
    }

    @Test
    public void testSendRecordsTaskCommitRecordFail() throws Exception {
        createWorkerTask();

        // Differentiate only by Kafka partition, so we can reuse conversion expectations
        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, "topic", 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, "topic", 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record3 = new SourceRecord(PARTITION, OFFSET, "topic", 3, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectTopicCreation(TOPIC);
        expectSendRecord();

        // Source task commit record failure will not cause the task to abort
        doNothing()
                .doThrow(new RuntimeException("Error committing record in source task"))
                .doNothing()
                .when(sourceTask).commitRecord(any(SourceRecord.class), any(RecordMetadata.class));

        workerTask.toSend = Arrays.asList(record1, record2, record3);
        workerTask.sendRecords();
        assertNull(workerTask.toSend);
    }

    @Test
    public void testSourceTaskIgnoresProducerException() throws Exception {
        createWorkerTaskWithErrorToleration();
        expectTopicCreation(TOPIC);

        //Use different offsets for each record, so we can verify all were committed
        final Map<String, Object> offset2 = Collections.singletonMap("key", 13);

        // send two records
        // record 1 will succeed
        // record 2 will invoke the producer's failure callback, but ignore the exception via retryOperator
        // and no ConnectException will be thrown
        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, TOPIC, 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, offset2, TOPIC, 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        expectOffsetFlush();
        expectPreliminaryCalls();

        when(producer.send(any(ProducerRecord.class), any(Callback.class)))
                .thenAnswer(producerSendAnswer(true))
                .thenAnswer(producerSendAnswer(false));

        //Send records and then commit offsets and verify both were committed and no exception
        workerTask.toSend = Arrays.asList(record1, record2);
        workerTask.sendRecords();
        workerTask.updateCommittableOffsets();
        workerTask.commitOffsets();

        //As of KAFKA-14079 all offsets should be committed, even for failed records (if ignored)
        //Only the last offset will be passed to the method as everything up to that point is committed
        //Before KAFKA-14079 offset 12 would have been passed and not 13 as it would have been unacked
        verify(offsetWriter).offset(PARTITION, offset2);
        verify(sourceTask).commitRecord(any(SourceRecord.class), isNull());

        //Double check to make sure all submitted records were cleared
        assertEquals(0, workerTask.submittedRecords.records.size());
    }

    @Test
    public void testSlowTaskStart() throws Exception {
        final CountDownLatch startupLatch = new CountDownLatch(1);
        final CountDownLatch finishStartupLatch = new CountDownLatch(1);

        createWorkerTask();

        doAnswer((Answer<Object>) invocation -> {
            startupLatch.countDown();
            ConcurrencyUtils.awaitLatch(finishStartupLatch, "Timeout waiting for main test thread to allow task startup to complete");
            return null;
        }).when(sourceTask).start(TASK_PROPS);

        expectOffsetFlush();

        workerTask.initialize(TASK_CONFIG);
        Future<?> workerTaskFuture = executor.submit(workerTask);

        // Stopping immediately while the other thread has work to do should result in no polling, no offset commits,
        // exiting the work thread immediately, and the stop() method will be invoked in the background thread since it
        // cannot be invoked immediately in the thread trying to stop the task.
        ConcurrencyUtils.awaitLatch(startupLatch, "Timeout waiting for task to begin startup");
        workerTask.stop();
        finishStartupLatch.countDown();
        assertTrue(workerTask.awaitStop(1000));

        workerTaskFuture.get();
        verify(offsetStore).start();
        verify(sourceTask).initialize(any(SourceTaskContext.class));
        verify(sourceTask).start(TASK_PROPS);
        verify(statusListener).onStartup(taskId);
        verify(statusListener).onShutdown(taskId);
        verify(sourceTask).stop();
        verifyClose();
    }

    @Test
    public void testCancel() {
        createWorkerTask();

        workerTask.cancel();
        verify(offsetReader).close();
        verify(producer).close(Duration.ZERO);
    }

    private TopicAdmin.TopicCreationResponse createdTopic(String topic) {
        Set<String> created = Collections.singleton(topic);
        Set<String> existing = Collections.emptySet();
        return new TopicAdmin.TopicCreationResponse(created, existing);
    }

    private void expectPreliminaryCalls() {
        expectConvertHeadersAndKeyValue(TOPIC, emptyHeaders());
        expectApplyTransformationChain();
    }

    private CountDownLatch expectEmptyPolls(final AtomicInteger count) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        // Note that we stub these to allow any number of calls because the thread will continue to
        // run. The count passed in + latch returned just makes sure we get *at least* that number of
        // calls
        when(sourceTask.poll()).thenAnswer((Answer<List<SourceRecord>>) invocation -> {
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
        doAnswer((Answer<List<SourceRecord>>) invocation -> {
            count.incrementAndGet();
            latch.countDown();
            Thread.sleep(10);
            return RECORDS;
        }).when(sourceTask).poll();

        // Fallout of the poll() call
        expectSendRecord();
        return latch;
    }

    private CountDownLatch expectPolls(int count) throws InterruptedException {
        return expectPolls(count, new AtomicInteger());
    }

    private void expectSendRecord() {
        expectSendRecordTaskCommitRecordSucceed();
    }

    private void expectSendRecordProducerCallbackFail() {
        expectSendRecord(TOPIC, false, emptyHeaders());
    }

    private void expectSendRecordTaskCommitRecordSucceed() {
        expectSendRecord(TOPIC, true, emptyHeaders());
    }

    private void expectSendRecord(String topic, boolean sendSuccess, Headers headers) {
        expectConvertHeadersAndKeyValue(topic, headers);

        expectApplyTransformationChain();

        if (sendSuccess) {
            // 2. As a result of a successful producer send callback, we'll notify the source task of the record commit
            expectTaskGetTopic();
        }

        doAnswer(producerSendAnswer(sendSuccess))
                .when(producer).send(any(ProducerRecord.class), any(Callback.class));
    }

    private Answer<Future<RecordMetadata>> producerSendAnswer(boolean sendSuccess) {
        return invocation -> {
            Callback cb = invocation.getArgument(1);
            if (sendSuccess) {
                cb.onCompletion(new RecordMetadata(new TopicPartition("foo", 0), 0, 0, 0L, 0, 0),
                        null);
            } else {
                cb.onCompletion(null, new TopicAuthorizationException("foo"));
            }

            return null;
        };
    }

    private void expectConvertHeadersAndKeyValue(String topic, Headers headers) {
        if (headers.iterator().hasNext()) {
            when(headerConverter.fromConnectHeader(anyString(), anyString(), eq(Schema.STRING_SCHEMA),
                    anyString()))
                    .thenAnswer((Answer<byte[]>) invocation -> {
                        String headerValue = invocation.getArgument(3, String.class);
                        return headerValue.getBytes(StandardCharsets.UTF_8);
                    });
        }

        when(keyConverter.fromConnectData(eq(topic), any(Headers.class), eq(KEY_SCHEMA), eq(KEY)))
                .thenReturn(SERIALIZED_KEY);
        when(valueConverter.fromConnectData(eq(topic), any(Headers.class), eq(RECORD_SCHEMA),
                eq(RECORD)))
                .thenReturn(SERIALIZED_RECORD);
    }

    private void expectApplyTransformationChain() {
        when(transformationChain.apply(any(), any(SourceRecord.class)))
                .thenAnswer(AdditionalAnswers.returnsSecondArg());
    }

    private void expectTaskGetTopic() {
        when(statusBackingStore.getTopic(anyString(), anyString())).thenAnswer((Answer<TopicStatus>) invocation -> {
            String connector = invocation.getArgument(0, String.class);
            String topic = invocation.getArgument(1, String.class);
            return new TopicStatus(topic, new ConnectorTaskId(connector, 0), Time.SYSTEM.milliseconds());
        });
    }

    private void verifyTaskGetTopic(int times) {
        ArgumentCaptor<String> connectorCapture = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> topicCapture = ArgumentCaptor.forClass(String.class);
        verify(statusBackingStore, times(times)).getTopic(connectorCapture.capture(), topicCapture.capture());

        assertEquals("job", connectorCapture.getValue());
        assertEquals(TOPIC, topicCapture.getValue());
    }

    private void expectBeginFlush() throws Exception {
        expectBeginFlush(() -> true);
    }

    private void expectBeginFlush(Supplier<Boolean> resultSupplier) throws Exception {
        when(offsetWriter.beginFlush(anyLong(), any(TimeUnit.class))).thenAnswer(ignored -> resultSupplier.get());
    }

    private void expectOffsetFlush() throws Exception {
        expectBeginFlush();
        expectOffsetFlush(true);
    }

    @SuppressWarnings("unchecked")
    private void expectOffsetFlush(Boolean... succeedList) throws Exception {
        Future<Void> flushFuture = mock(Future.class);
        when(offsetWriter.doFlush(any(org.apache.kafka.connect.util.Callback.class))).thenReturn(flushFuture);
        LinkedList<Boolean> succeedQueue = new LinkedList<>(Arrays.asList(succeedList));

        doAnswer(invocationOnMock -> {
            boolean succeed = succeedQueue.pop();
            if (succeed) {
                return null;
            } else {
                throw new TimeoutException();
            }
        }).when(flushFuture).get(anyLong(), any(TimeUnit.class));
    }

    private void expectEmptyOffsetFlush() throws Exception {
        expectBeginFlush(() -> false);
    }

    private void verifyOffsetFlush(boolean succeed) throws Exception {
        verifyOffsetFlush(succeed, 1);
    }

    private void verifyOffsetFlush(boolean succeed, int times) throws Exception {
        // Should throw for failure
        if (succeed) {
            verify(sourceTask, atLeast(times)).commit();
        } else {
            verify(offsetWriter, atLeast(times)).cancelFlush();
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

    private RecordHeaders emptyHeaders() {
        return new RecordHeaders();
    }

    private abstract static class TestSourceTask extends SourceTask {
    }

    private void verifyCleanStartup() {
        verify(offsetStore).start();
        verify(sourceTask).initialize(any(SourceTaskContext.class));
        verify(sourceTask).start(TASK_PROPS);
        verify(statusListener).onStartup(taskId);
    }

    private void verifyClose() {
        verify(producer).close(any(Duration.class));
        verify(admin).close(any(Duration.class));
        verify(transformationChain).close();
        verify(offsetReader).close();
        verify(offsetStore).stop();

        try {
            verify(headerConverter).close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void expectTopicCreation(String topic) {
        if (config.topicCreationEnable()) {
            when(admin.describeTopics(topic)).thenReturn(Collections.emptyMap());
            when(admin.createOrFindTopics(any(NewTopic.class))).thenReturn(createdTopic(topic));
        }
    }

    private void verifyTopicCreation(String... topics) {
        if (config.topicCreationEnable()) {
            ArgumentCaptor<NewTopic> newTopicCapture = ArgumentCaptor.forClass(NewTopic.class);

            verify(admin).createOrFindTopics(newTopicCapture.capture());
            assertArrayEquals(topics, newTopicCapture.getAllValues()
                    .stream()
                    .map(NewTopic::name)
                    .toArray(String[]::new));
        }
    }

    private void assertShouldSkipCommit() {
        assertFalse(workerTask.shouldCommitOffsets());

        try (LogCaptureAppender committerAppender = LogCaptureAppender.createAndRegister(SourceTaskOffsetCommitter.class);
             LogCaptureAppender taskAppender = LogCaptureAppender.createAndRegister(WorkerSourceTask.class)) {
            committerAppender.setClassLoggerToTrace(SourceTaskOffsetCommitter.class);
            taskAppender.setClassLoggerToTrace(WorkerSourceTask.class);
            SourceTaskOffsetCommitter.commit(workerTask);
            assertEquals(Collections.emptyList(), taskAppender.getMessages());

            List<String> committerMessages = committerAppender.getMessages();
            assertEquals(1, committerMessages.size());
            assertTrue(committerMessages.get(0).contains("Skipping offset commit"));
        }
    }
}
