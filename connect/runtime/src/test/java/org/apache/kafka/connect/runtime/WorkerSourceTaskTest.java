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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.WorkerSourceTask.SourceTaskMetricsGroup;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.TestFuture;
import org.apache.kafka.connect.util.ThreadedTest;
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
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
public class WorkerSourceTaskTest extends ThreadedTest {
    private static final String TOPIC = "topic";
    private static final Map<String, byte[]> PARTITION = Collections.singletonMap("key", "partition".getBytes());
    private static final Map<String, Integer> OFFSET = Collections.singletonMap("key", 12);
    private static final Map<String, Integer> OFFSET2 = Collections.singletonMap("key", 13);

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
    private Plugins plugins;
    private MockConnectMetrics metrics;
    @Mock private SourceTask sourceTask;
    @Mock private Converter keyConverter;
    @Mock private Converter valueConverter;
    @Mock private HeaderConverter headerConverter;
    @Mock private TransformationChain<SourceRecord> transformationChain;
    @Mock private KafkaProducer<byte[], byte[]> producer;
    @Mock private OffsetStorageReader offsetReader;
    @Mock private OffsetStorageWriter offsetWriter;
    private WorkerSourceTask workerTask;
    @Mock private Future<RecordMetadata> sendFuture;
    @MockStrict private TaskStatus.Listener statusListener;

    private Capture<org.apache.kafka.clients.producer.Callback> producerCallbacks;

    private static final Map<String, String> TASK_PROPS = new HashMap<>();
    static {
        TASK_PROPS.put(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());
    }
    private static final TaskConfig TASK_CONFIG = new TaskConfig(TASK_PROPS);

    private static final List<SourceRecord> RECORDS = Arrays.asList(
            new SourceRecord(PARTITION, OFFSET, "topic", null, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD)
    );
    private static final List<SourceRecord> RECORDS2 = Arrays.asList(
            new SourceRecord(PARTITION, OFFSET2, "topic", null, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD)
    );

    @Override
    public void setup() {
        super.setup();
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter.schemas.enable", "false");
        workerProps.put("internal.value.converter.schemas.enable", "false");
        workerProps.put("offset.storage.file.filename", "/tmp/connect.offsets");
        plugins = new Plugins(workerProps);
        config = new StandaloneConfig(workerProps);
        producerCallbacks = EasyMock.newCapture();
        metrics = new MockConnectMetrics();
    }

    @After
    public void tearDown() {
        if (metrics != null) metrics.stop();
    }

    private void createWorkerTask() {
        createWorkerTask(TargetState.STARTED);
    }

    private void createWorkerTask(TargetState initialState) {
        workerTask = new WorkerSourceTask(taskId, sourceTask, statusListener, initialState, keyConverter, valueConverter, headerConverter,
                transformationChain, producer, offsetReader, offsetWriter, config, metrics, plugins.delegatingLoader(), Time.SYSTEM);
    }

    @Test
    public void testStartPaused() throws Exception {
        final CountDownLatch pauseLatch = new CountDownLatch(1);

        createWorkerTask(TargetState.PAUSED);

        statusListener.onPause(taskId);
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() throws Throwable {
                pauseLatch.countDown();
                return null;
            }
        });

        producer.close(EasyMock.anyLong(), EasyMock.anyObject(TimeUnit.class));
        EasyMock.expectLastCall();

        transformationChain.close();
        EasyMock.expectLastCall();

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

        sourceTask.initialize(EasyMock.anyObject(SourceTaskContext.class));
        EasyMock.expectLastCall();
        sourceTask.start(TASK_PROPS);
        EasyMock.expectLastCall();
        statusListener.onStartup(taskId);
        EasyMock.expectLastCall();

        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch pollLatch = expectPolls(10, count);
        // In this test, we don't flush, so nothing goes any further than the offset writer

        statusListener.onPause(taskId);
        EasyMock.expectLastCall();

        sourceTask.stop();
        EasyMock.expectLastCall();
        Capture<List<SourceRecord>> offsetsFlushedCap = expectOffsetFlush(true);

        statusListener.onShutdown(taskId);
        EasyMock.expectLastCall();

        producer.close(EasyMock.anyLong(), EasyMock.anyObject(TimeUnit.class));
        EasyMock.expectLastCall();

        transformationChain.close();
        EasyMock.expectLastCall();

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

        List<SourceRecord> offsetsFlushed = offsetsFlushedCap.getValue();
        assertEquals(concatWithSelf(RECORDS, count.get()), offsetsFlushed);
    }

    /*
    Unfortunately it is (probably) not feasible (throughput-wise) to guarantee that everything polled will be included
    in a offsets flush. Currently it is possible that recently polled records will not have their offsets included in
    a flush of offsets. But the records that have had their offsets flushed should be correctly reported to the source
    task (offsetsFlushedAndAcknowledged).
    This test thoroughly provokes a scenario where the last polled records are not included in a offsets flush, and
    verifies that only the records that actually had their offsets flushed are reported as such.
     */
    @Test
    public void testOffsetsFlushedNotIncludingLatestPoll() throws Exception {
        createWorkerTask();

        sourceTask.initialize(EasyMock.anyObject(SourceTaskContext.class));
        EasyMock.expectLastCall();
        sourceTask.start(TASK_PROPS);
        EasyMock.expectLastCall();
        statusListener.onStartup(taskId);
        EasyMock.expectLastCall();

        final CountDownLatch offsetFlushLatch = new CountDownLatch(1);
        final CountDownLatch allPolledLatch = new CountDownLatch(1);
        final PolledRecordsAndOffsetCaptures polledRecordsAndOffsetCaptures = expectPolls(2, offsetFlushLatch, allPolledLatch, RECORDS, RECORDS2);

        final Map<Map<String, Object>, Map<String, Object>> writtenAndFlushedOffsets = new HashMap<>();
        offsetWriter.beginFlush();
        EasyMock.expectLastCall().andAnswer(new IAnswer<Boolean>() {
            @Override
            public Boolean answer() throws Throwable {
                writtenAndFlushedOffsets.putAll(polledRecordsAndOffsetCaptures.writtenOffsetsSinceLast());
                return true;
            }
        });
        offsetWriter.doFlush(EasyMock.anyObject(Callback.class));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Future<Void>>() {
            @Override
            public Future<Void> answer() throws Throwable {
                TestFuture<Void> future = new TestFuture<Void>();
                future.resolveOnGet((Void) null);
                return future;
            }
        });
        final List<List<SourceRecord>> polledRecordsAtTimeOfOffsetsFlushed = new ArrayList<>();
        final Map<Map<String, Object>, Map<String, Object>> writtenAndFlushedOffsetsAtTimeOfOffsetsFlushed = new HashMap<>();
        Capture<List<SourceRecord>> recordsReportedHavingOffsetsFlushed = EasyMock.newCapture();
        sourceTask.offsetsFlushedAndAcknowledged(EasyMock.capture(recordsReportedHavingOffsetsFlushed));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() throws Throwable {
                polledRecordsAtTimeOfOffsetsFlushed.addAll(polledRecordsAndOffsetCaptures.polledRecordsSinceLast());
                writtenAndFlushedOffsetsAtTimeOfOffsetsFlushed.putAll(writtenAndFlushedOffsets);
                offsetFlushLatch.countDown();
                return null;
            }
        });

        sourceTask.stop();
        EasyMock.expectLastCall();
        expectOffsetFlush(true);

        statusListener.onShutdown(taskId);
        EasyMock.expectLastCall();

        producer.close(EasyMock.anyLong(), EasyMock.anyObject(TimeUnit.class));
        EasyMock.expectLastCall();

        transformationChain.close();
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        assertTrue(awaitLatch(allPolledLatch));
        // To make test green, move offsetFlushLatch.countDown() here (see above)
        // offsetFlushLatch.countDown();

        // Simulating SourceTaskOffsetCommitter calling flushOffsets at will
        workerTask.commitOffsets();

        assertTrue(awaitLatch(offsetFlushLatch));
        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();

        PowerMock.verifyAll();

        // We wanted to trigger that RECORDS was polled, and then RECORDS2 was polled
        List<List<SourceRecord>> wantedPolledRecordListsAtTimeOfOffsetsFlushed = new ArrayList<>();
        wantedPolledRecordListsAtTimeOfOffsetsFlushed.add(RECORDS);
        wantedPolledRecordListsAtTimeOfOffsetsFlushed.add(RECORDS2);

        // We wanted to trigger that, only RECORDS (not RECORDS2) made it to the offsets flush
        List<List<SourceRecord>> wantedRecordListsWithOffsetsIncludedInOffsetsFlush = new ArrayList<>();
        wantedRecordListsWithOffsetsIncludedInOffsetsFlush.add(RECORDS);

        // Just checking that we triggered what we wanted to trigger, or the test does not test want it wants to test
        assertThat(polledRecordsAtTimeOfOffsetsFlushed, equalTo(wantedPolledRecordListsAtTimeOfOffsetsFlushed));
        assertThat(writtenAndFlushedOffsetsAtTimeOfOffsetsFlushed, equalTo(extractOffsets(wantedRecordListsWithOffsetsIncludedInOffsetsFlush)));

        // Now verify that the correct list of records (RECORDS) were reported as having had their offsets flushed
        assertThat(recordsReportedHavingOffsetsFlushed.getValues(), equalTo(wantedRecordListsWithOffsetsIncludedInOffsetsFlush));
    }

    @Test
    public void testPollsInBackground() throws Exception {
        createWorkerTask();

        sourceTask.initialize(EasyMock.anyObject(SourceTaskContext.class));
        EasyMock.expectLastCall();
        sourceTask.start(TASK_PROPS);
        EasyMock.expectLastCall();
        statusListener.onStartup(taskId);
        EasyMock.expectLastCall();

        AtomicInteger count = new AtomicInteger();
        final CountDownLatch pollLatch = expectPolls(10, count);
        // In this test, we don't flush, so nothing goes any further than the offset writer

        sourceTask.stop();
        EasyMock.expectLastCall();
        Capture<List<SourceRecord>> offsetsFlushedCap = expectOffsetFlush(true);

        statusListener.onShutdown(taskId);
        EasyMock.expectLastCall();

        producer.close(EasyMock.anyLong(), EasyMock.anyObject(TimeUnit.class));
        EasyMock.expectLastCall();

        transformationChain.close();
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        assertTrue(awaitLatch(pollLatch));
        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();
        assertPollMetrics(10);

        PowerMock.verifyAll();

        List<SourceRecord> offsetsFlushed = offsetsFlushedCap.getValue();
        assertEquals(concatWithSelf(RECORDS, count.get()), offsetsFlushed);
    }

    @Test
    public void testFailureInPoll() throws Exception {
        createWorkerTask();

        sourceTask.initialize(EasyMock.anyObject(SourceTaskContext.class));
        EasyMock.expectLastCall();
        sourceTask.start(TASK_PROPS);
        EasyMock.expectLastCall();
        statusListener.onStartup(taskId);
        EasyMock.expectLastCall();

        final CountDownLatch pollLatch = new CountDownLatch(1);
        final RuntimeException exception = new RuntimeException();
        EasyMock.expect(sourceTask.poll()).andAnswer(new IAnswer<List<SourceRecord>>() {
            @Override
            public List<SourceRecord> answer() throws Throwable {
                pollLatch.countDown();
                throw exception;
            }
        });

        statusListener.onFailure(taskId, exception);
        EasyMock.expectLastCall();

        sourceTask.stop();
        EasyMock.expectLastCall();
        Capture<List<SourceRecord>> offsetsFlushedCap = expectOffsetFlush(true);

        producer.close(EasyMock.anyLong(), EasyMock.anyObject(TimeUnit.class));
        EasyMock.expectLastCall();

        transformationChain.close();
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        Future<?> taskFuture = executor.submit(workerTask);

        assertTrue(awaitLatch(pollLatch));
        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();
        assertPollMetrics(0);

        PowerMock.verifyAll();

        List<SourceRecord> offsetsFlushed = offsetsFlushedCap.getValue();
        assertEquals(new ArrayList<>(), offsetsFlushed);
    }

    @Test
    public void testOffsetFlush() throws Exception {
        // Test that the task flushes offsets properly when prompted
        createWorkerTask();

        sourceTask.initialize(EasyMock.anyObject(SourceTaskContext.class));
        EasyMock.expectLastCall();
        sourceTask.start(TASK_PROPS);
        EasyMock.expectLastCall();
        statusListener.onStartup(taskId);
        EasyMock.expectLastCall();

        // We'll wait for some data, then trigger a flush
        final CountDownLatch pollLatch = expectPolls(1);
        Capture<List<SourceRecord>> offsetsFlushedCap1 = expectOffsetFlush(true);

        sourceTask.stop();
        EasyMock.expectLastCall();
        Capture<List<SourceRecord>> offsetsFlushedCap2 = expectOffsetFlush(true);

        statusListener.onShutdown(taskId);
        EasyMock.expectLastCall();

        producer.close(EasyMock.anyLong(), EasyMock.anyObject(TimeUnit.class));
        EasyMock.expectLastCall();

        transformationChain.close();
        EasyMock.expectLastCall();

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

        List<SourceRecord> offsetsFlushed = offsetsFlushedCap1.getValue();
        List<SourceRecord> offsetsFlushed2 = offsetsFlushedCap2.getValue();
        offsetsFlushed.addAll(offsetsFlushed2);
        assertEquals(RECORDS, offsetsFlushed);
    }

    @Test
    public void testOffsetFlushFailure() throws Exception {
        // Test that the task flushes offsets properly when prompted
        createWorkerTask();

        sourceTask.initialize(EasyMock.anyObject(SourceTaskContext.class));
        EasyMock.expectLastCall();
        sourceTask.start(TASK_PROPS);
        EasyMock.expectLastCall();
        statusListener.onStartup(taskId);
        EasyMock.expectLastCall();

        // We'll wait for some data, then trigger a flush
        final CountDownLatch pollLatch = expectPolls(1);
        Capture<List<SourceRecord>> offsetsFlushedCap = expectOffsetFlush(true);

        sourceTask.stop();
        EasyMock.expectLastCall();
        expectOffsetFlush(false);

        statusListener.onShutdown(taskId);
        EasyMock.expectLastCall();

        producer.close(EasyMock.anyLong(), EasyMock.anyObject(TimeUnit.class));
        EasyMock.expectLastCall();

        transformationChain.close();
        EasyMock.expectLastCall();

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

        List<SourceRecord> offsetsFlushed = offsetsFlushedCap.getValue();
        assertEquals(new ArrayList<>(), offsetsFlushed);
    }

    @Test
    public void testSendRecordsConvertsData() throws Exception {
        createWorkerTask();

        List<SourceRecord> records = new ArrayList<>();
        // Can just use the same record for key and value
        records.add(new SourceRecord(PARTITION, OFFSET, "topic", null, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD));

        Capture<ProducerRecord<byte[], byte[]>> sent = expectSendRecordAnyTimes();

        PowerMock.replayAll();

        Whitebox.setInternalState(workerTask, "toSend", records);
        Whitebox.invokeMethod(workerTask, "sendRecords");
        assertEquals(SERIALIZED_KEY, sent.getValue().key());
        assertEquals(SERIALIZED_RECORD, sent.getValue().value());

        PowerMock.verifyAll();
    }

    @Test
    public void testSendRecordsPropagatesTimestamp() throws Exception {
        final Long timestamp = System.currentTimeMillis();

        createWorkerTask();

        List<SourceRecord> records = Collections.singletonList(
                new SourceRecord(PARTITION, OFFSET, "topic", null, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD, timestamp)
        );

        Capture<ProducerRecord<byte[], byte[]>> sent = expectSendRecordAnyTimes();

        PowerMock.replayAll();

        Whitebox.setInternalState(workerTask, "toSend", records);
        Whitebox.invokeMethod(workerTask, "sendRecords");
        assertEquals(timestamp, sent.getValue().timestamp());

        PowerMock.verifyAll();
    }

    @Test(expected = InvalidRecordException.class)
    public void testSendRecordsCorruptTimestamp() throws Exception {
        final Long timestamp = -3L;
        createWorkerTask();

        List<SourceRecord> records = Collections.singletonList(
                new SourceRecord(PARTITION, OFFSET, "topic", null, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD, timestamp)
        );

        Capture<ProducerRecord<byte[], byte[]>> sent = expectSendRecordAnyTimes();

        PowerMock.replayAll();

        Whitebox.setInternalState(workerTask, "toSend", records);
        Whitebox.invokeMethod(workerTask, "sendRecords");
        assertEquals(null, sent.getValue().timestamp());

        PowerMock.verifyAll();
    }

    @Test
    public void testSendRecordsNoTimestamp() throws Exception {
        final Long timestamp = -1L;
        createWorkerTask();

        List<SourceRecord> records = Collections.singletonList(
                new SourceRecord(PARTITION, OFFSET, "topic", null, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD, timestamp)
        );

        Capture<ProducerRecord<byte[], byte[]>> sent = expectSendRecordAnyTimes();

        PowerMock.replayAll();

        Whitebox.setInternalState(workerTask, "toSend", records);
        Whitebox.invokeMethod(workerTask, "sendRecords");
        assertEquals(null, sent.getValue().timestamp());

        PowerMock.verifyAll();
    }

    @Test
    public void testSendRecordsRetries() throws Exception {
        createWorkerTask();

        // Differentiate only by Kafka partition so we can reuse conversion expectations
        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, "topic", 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, "topic", 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record3 = new SourceRecord(PARTITION, OFFSET, "topic", 3, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        // First round
        expectSendRecordOnce(false);
        // Any Producer retriable exception should work here
        expectSendRecordSyncFailure(new org.apache.kafka.common.errors.TimeoutException("retriable sync failure"));

        // Second round
        expectSendRecordOnce(true);
        expectSendRecordOnce(false);

        PowerMock.replayAll();

        // Try to send 3, make first pass, second fail. Should save last two
        Whitebox.setInternalState(workerTask, "toSend", Arrays.asList(record1, record2, record3));
        Whitebox.invokeMethod(workerTask, "sendRecords");
        assertEquals(true, Whitebox.getInternalState(workerTask, "lastSendFailed"));
        assertEquals(Arrays.asList(record2, record3), Whitebox.getInternalState(workerTask, "toSend"));

        // Next they all succeed
        Whitebox.invokeMethod(workerTask, "sendRecords");
        assertEquals(false, Whitebox.getInternalState(workerTask, "lastSendFailed"));
        assertNull(Whitebox.getInternalState(workerTask, "toSend"));

        PowerMock.verifyAll();
    }

    @Test
    public void testSendRecordsTaskRecordAcknowledgedFail() throws Exception {
        createWorkerTask();

        // Differentiate only by Kafka partition so we can reuse conversion expectations
        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, "topic", 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, "topic", 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record3 = new SourceRecord(PARTITION, OFFSET, "topic", 3, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        // Source task record acknowledged failure will not cause the task to abort
        expectSendRecordOnce(false);
        expectSendRecordTaskRecordAcknowledgedFail(false, false);
        expectSendRecordOnce(false);

        PowerMock.replayAll();

        Whitebox.setInternalState(workerTask, "toSend", Arrays.asList(record1, record2, record3));
        Whitebox.invokeMethod(workerTask, "sendRecords");
        assertEquals(false, Whitebox.getInternalState(workerTask, "lastSendFailed"));
        assertNull(Whitebox.getInternalState(workerTask, "toSend"));

        PowerMock.verifyAll();
    }

    @Test
    public void testSlowTaskStart() throws Exception {
        final CountDownLatch startupLatch = new CountDownLatch(1);
        final CountDownLatch finishStartupLatch = new CountDownLatch(1);

        createWorkerTask();

        sourceTask.initialize(EasyMock.anyObject(SourceTaskContext.class));
        EasyMock.expectLastCall();
        sourceTask.start(TASK_PROPS);
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                startupLatch.countDown();
                assertTrue(awaitLatch(finishStartupLatch));
                return null;
            }
        });

        statusListener.onStartup(taskId);
        EasyMock.expectLastCall();

        sourceTask.stop();
        EasyMock.expectLastCall();
        Capture<List<SourceRecord>> offsetsFlushedCap = expectOffsetFlush(true);

        statusListener.onShutdown(taskId);
        EasyMock.expectLastCall();

        producer.close(EasyMock.anyLong(), EasyMock.anyObject(TimeUnit.class));
        EasyMock.expectLastCall();

        transformationChain.close();
        EasyMock.expectLastCall();

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

        List<SourceRecord> offsetsFlushed = offsetsFlushedCap.getValue();
        assertEquals(new ArrayList<>(), offsetsFlushed);
    }

    @Test
    public void testMetricsGroup() {
        SourceTaskMetricsGroup group = new SourceTaskMetricsGroup(taskId, metrics);
        SourceTaskMetricsGroup group1 = new SourceTaskMetricsGroup(taskId1, metrics);
        for (int i = 0; i != 10; ++i) {
            group.recordPoll(100, 1000 + i * 100);
            group.recordWrite(10);
        }
        for (int i = 0; i != 20; ++i) {
            group1.recordPoll(100, 1000 + i * 100);
            group1.recordWrite(10);
        }
        assertEquals(1900.0, metrics.currentMetricValueAsDouble(group.metricGroup(), "poll-batch-max-time-ms"), 0.001d);
        assertEquals(1450.0, metrics.currentMetricValueAsDouble(group.metricGroup(), "poll-batch-avg-time-ms"), 0.001d);
        assertEquals(33.333, metrics.currentMetricValueAsDouble(group.metricGroup(), "source-record-poll-rate"), 0.001d);
        assertEquals(1000, metrics.currentMetricValueAsDouble(group.metricGroup(), "source-record-poll-total"), 0.001d);
        assertEquals(3.3333, metrics.currentMetricValueAsDouble(group.metricGroup(), "source-record-write-rate"), 0.001d);
        assertEquals(100, metrics.currentMetricValueAsDouble(group.metricGroup(), "source-record-write-total"), 0.001d);
        assertEquals(900.0, metrics.currentMetricValueAsDouble(group.metricGroup(), "source-record-active-count"), 0.001d);

        // Close the group
        group.close();

        for (MetricName metricName : group.metricGroup().metrics().metrics().keySet()) {
            // Metrics for this group should no longer exist
            assertFalse(group.metricGroup().groupId().includes(metricName));
        }
        // Sensors for this group should no longer exist
        assertNull(group.metricGroup().metrics().getSensor("sink-record-read"));
        assertNull(group.metricGroup().metrics().getSensor("sink-record-send"));
        assertNull(group.metricGroup().metrics().getSensor("sink-record-active-count"));
        assertNull(group.metricGroup().metrics().getSensor("partition-count"));
        assertNull(group.metricGroup().metrics().getSensor("offset-seq-number"));
        assertNull(group.metricGroup().metrics().getSensor("offset-commit-completion"));
        assertNull(group.metricGroup().metrics().getSensor("offset-commit-completion-skip"));
        assertNull(group.metricGroup().metrics().getSensor("put-batch-time"));

        assertEquals(2900.0, metrics.currentMetricValueAsDouble(group1.metricGroup(), "poll-batch-max-time-ms"), 0.001d);
        assertEquals(1950.0, metrics.currentMetricValueAsDouble(group1.metricGroup(), "poll-batch-avg-time-ms"), 0.001d);
        assertEquals(66.667, metrics.currentMetricValueAsDouble(group1.metricGroup(), "source-record-poll-rate"), 0.001d);
        assertEquals(2000, metrics.currentMetricValueAsDouble(group1.metricGroup(), "source-record-poll-total"), 0.001d);
        assertEquals(6.667, metrics.currentMetricValueAsDouble(group1.metricGroup(), "source-record-write-rate"), 0.001d);
        assertEquals(200, metrics.currentMetricValueAsDouble(group1.metricGroup(), "source-record-write-total"), 0.001d);
        assertEquals(1800.0, metrics.currentMetricValueAsDouble(group1.metricGroup(), "source-record-active-count"), 0.001d);
    }

    private CountDownLatch expectPolls(int minimum, final AtomicInteger count) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(minimum);
        // Note that we stub these to allow any number of calls because the thread will continue to
        // run. The count passed in + latch returned just makes sure we get *at least* that number of
        // calls
        EasyMock.expect(sourceTask.poll())
                .andStubAnswer(new IAnswer<List<SourceRecord>>() {
                    @Override
                    public List<SourceRecord> answer() throws Throwable {
                        count.incrementAndGet();
                        latch.countDown();
                        Thread.sleep(10);
                        return RECORDS;
                    }
                });
        // Fallout of the poll() call
        expectSendRecordAnyTimes();
        return latch;
    }

    private CountDownLatch expectPolls(int minimum) throws InterruptedException {
        return expectPolls(minimum, new AtomicInteger());
    }

    private static class PolledRecordsAndOffsetCaptures {
        List<List<SourceRecord>> polledRecords;
        Capture<Map<String, ?>> offsetPartitionsWritten;
        Capture<Map<String, ?>> offsetOffsetsWritten;

        PolledRecordsAndOffsetCaptures() {
            polledRecords = new ArrayList<>();
            offsetPartitionsWritten = EasyMock.newCapture();
            offsetOffsetsWritten = EasyMock.newCapture();
        }

        List<List<SourceRecord>> polledRecordsSinceLast() {
            List<List<SourceRecord>> result = new ArrayList<>(polledRecords);
            polledRecords.clear();
            return result;
        }

        Map<Map<String, Object>, Map<String, Object>> writtenOffsetsSinceLast() {
            Map<Map<String, Object>, Map<String, Object>> result = new HashMap<>();
            for (int i = 0; i < offsetPartitionsWritten.getValues().size(); i++) {
                result.put((Map<String, Object>) offsetPartitionsWritten.getValues().get(i), (Map<String, Object>) offsetOffsetsWritten.getValues().get(i));
            }
            offsetPartitionsWritten.reset();
            offsetOffsetsWritten.reset();
            return result;
        }

    }
    private PolledRecordsAndOffsetCaptures expectPolls(int waitForConvertValueLatchOnConvertNo, CountDownLatch convertValueLatch, final CountDownLatch allPolledLatch, final List<SourceRecord>... recordss) throws InterruptedException {
        final AtomicInteger count = new AtomicInteger();
        final PolledRecordsAndOffsetCaptures polledRecordsAndOffsetCaptures = new PolledRecordsAndOffsetCaptures();
        // Note that we stub these to allow any number of calls because the thread will continue to
        // run. The count passed in + latch returned just makes sure we get *at least* that number of
        // calls
        EasyMock.expect(sourceTask.poll())
                .andAnswer(new IAnswer<List<SourceRecord>>() {
                    @Override
                    public List<SourceRecord> answer() throws Throwable {
                        int recordsToAnswer = count.getAndIncrement();
                        List<SourceRecord> response = (recordsToAnswer < recordss.length) ? recordss[recordsToAnswer] : new ArrayList();
                        polledRecordsAndOffsetCaptures.polledRecords.add(response);
                        if (recordsToAnswer == recordss.length - 1) allPolledLatch.countDown();
                        return response;
                    }
                })
                .anyTimes();
        // Fallout of the poll() call
        expectSendRecord(waitForConvertValueLatchOnConvertNo, convertValueLatch, polledRecordsAndOffsetCaptures.offsetPartitionsWritten, polledRecordsAndOffsetCaptures.offsetOffsetsWritten);
        return polledRecordsAndOffsetCaptures;
    }

    @SuppressWarnings("unchecked")
    private void expectSendRecordSyncFailure(Throwable error) throws InterruptedException {
        expectConvertKeyValue(false, Integer.MAX_VALUE, null);
        expectApplyTransformationChain(false);

        offsetWriter.offset(PARTITION, OFFSET);
        PowerMock.expectLastCall();

        EasyMock.expect(
                producer.send(EasyMock.anyObject(ProducerRecord.class),
                        EasyMock.anyObject(org.apache.kafka.clients.producer.Callback.class)))
                .andThrow(error);
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecordAnyTimes() throws InterruptedException {
        return expectSendRecordTaskRecordAcknowledgedSucceed(true, false);
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecordOnce(boolean isRetry) throws InterruptedException {
        return expectSendRecordTaskRecordAcknowledgedSucceed(false, isRetry);
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecordTaskRecordAcknowledgedSucceed(boolean anyTimes, boolean isRetry) throws InterruptedException {
        return expectSendRecord(anyTimes, isRetry, true);
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecordTaskRecordAcknowledgedFail(boolean anyTimes, boolean isRetry) throws InterruptedException {
        return expectSendRecord(anyTimes, isRetry, false);
    }

    @SuppressWarnings("unchecked")
    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecord(boolean anyTimes, boolean isRetry, boolean succeed) throws InterruptedException {
        expectConvertKeyValue(anyTimes, Integer.MAX_VALUE, null);
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
        IAnswer<Future<RecordMetadata>> expectResponse = new IAnswer<Future<RecordMetadata>>() {
            @Override
            public Future<RecordMetadata> answer() throws Throwable {
                synchronized (producerCallbacks) {
                    for (org.apache.kafka.clients.producer.Callback cb : producerCallbacks.getValues()) {
                        cb.onCompletion(new RecordMetadata(new TopicPartition("foo", 0), 0, 0,
                                                           0L, 0L, 0, 0), null);
                    }
                    producerCallbacks.reset();
                }
                return sendFuture;
            }
        };
        if (anyTimes)
            expect.andStubAnswer(expectResponse);
        else
            expect.andAnswer(expectResponse);

        // 3. As a result of a successful producer send callback, we'll notify the source task of the record acknowledgement
        expectTaskRecordAcknowledged(anyTimes, succeed);

        return sent;
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecord(int waitForConvertValueLatchOnConvertNo, CountDownLatch convertValueLatch, Capture<Map<String, ?>> offsetPartitions, Capture<Map<String, ?>> offsetOffsets) throws InterruptedException {
        expectConvertKeyValue(true, waitForConvertValueLatchOnConvertNo, convertValueLatch);
        expectApplyTransformationChain(true);

        Capture<ProducerRecord<byte[], byte[]>> sent = EasyMock.newCapture();

        // 1. Offset data is passed to the offset storage.
        offsetWriter.offset(EasyMock.capture(offsetPartitions), EasyMock.capture(offsetOffsets));
        PowerMock.expectLastCall().anyTimes();

        // 2. Converted data passed to the producer, which will need callbacks invoked for flush to work
        IExpectationSetters<Future<RecordMetadata>> expect = EasyMock.expect(
                producer.send(EasyMock.capture(sent),
                        EasyMock.capture(producerCallbacks)));
        IAnswer<Future<RecordMetadata>> expectResponse = new IAnswer<Future<RecordMetadata>>() {
            @Override
            public Future<RecordMetadata> answer() throws Throwable {
                synchronized (producerCallbacks) {
                    for (org.apache.kafka.clients.producer.Callback cb : producerCallbacks.getValues()) {
                        cb.onCompletion(new RecordMetadata(new TopicPartition("foo", 0), 0, 0,
                                0L, 0L, 0, 0), null);
                    }
                    producerCallbacks.reset();
                }
                return sendFuture;
            }
        };
        expect.andStubAnswer(expectResponse);

        // 3. As a result of a successful producer send callback, we'll notify the source task of the record acknowledgement
        expectTaskRecordAcknowledged(true, true);

        return sent;
    }

    private void expectConvertKeyValue(boolean anyTimes, final int waitForConvertValueLatchOnConvertNo, final CountDownLatch convertValueLatch) {
        IExpectationSetters<byte[]> convertKeyExpect = EasyMock.expect(keyConverter.fromConnectData(TOPIC, KEY_SCHEMA, KEY));
        if (anyTimes)
            convertKeyExpect.andStubReturn(SERIALIZED_KEY);
        else
            convertKeyExpect.andReturn(SERIALIZED_KEY);
        IExpectationSetters<byte[]> convertValueExpect = EasyMock.expect(valueConverter.fromConnectData(TOPIC, RECORD_SCHEMA, RECORD));
        if (anyTimes)
            convertValueExpect.andStubAnswer(new IAnswer<byte[]>() {
                final AtomicInteger count = new AtomicInteger();
                @Override
                public byte[] answer() throws Throwable {
                    if (waitForConvertValueLatchOnConvertNo == count.incrementAndGet()) {
                        convertValueLatch.await();
                    }
                    return SERIALIZED_RECORD;
                }
            });
        else
            convertValueExpect.andReturn(SERIALIZED_RECORD);
    }

    private void expectApplyTransformationChain(boolean anyTimes) {
        final Capture<SourceRecord> recordCapture = EasyMock.newCapture();
        IExpectationSetters<SourceRecord> convertKeyExpect = EasyMock.expect(transformationChain.apply(EasyMock.capture(recordCapture)));
        if (anyTimes)
            convertKeyExpect.andStubAnswer(new IAnswer<SourceRecord>() {
                @Override
                public SourceRecord answer() {
                    return recordCapture.getValue();
                }
            });
        else
            convertKeyExpect.andAnswer(new IAnswer<SourceRecord>() {
                @Override
                public SourceRecord answer() {
                    return recordCapture.getValue();
                }
            });
    }

    private void expectTaskRecordAcknowledged(boolean anyTimes, boolean succeed) throws InterruptedException {
        sourceTask.recordSentAndAcknowledged(EasyMock.anyObject(SourceRecord.class));
        IExpectationSetters<Void> expect = EasyMock.expectLastCall();
        if (!succeed) {
            expect = expect.andThrow(new RuntimeException("Error acknowledging record in source task"));
        }
        if (anyTimes) {
            expect.anyTimes();
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
    private Capture<List<SourceRecord>> expectOffsetFlush(boolean succeed) throws Exception {
        EasyMock.expect(offsetWriter.beginFlush()).andReturn(true);
        Future<Void> flushFuture = PowerMock.createMock(Future.class);
        EasyMock.expect(offsetWriter.doFlush(EasyMock.anyObject(Callback.class))).andReturn(flushFuture);
        // Should throw for failure
        IExpectationSetters<Void> futureGetExpect = EasyMock.expect(
                flushFuture.get(EasyMock.anyLong(), EasyMock.anyObject(TimeUnit.class)));
        if (succeed) {
            Capture<List<SourceRecord>> offsetsFlushed = EasyMock.newCapture();
            sourceTask.offsetsFlushedAndAcknowledged(EasyMock.capture(offsetsFlushed));
            EasyMock.expectLastCall();
            futureGetExpect.andReturn(null);
            return offsetsFlushed;
        } else {
            futureGetExpect.andThrow(new TimeoutException());
            offsetWriter.cancelFlush();
            PowerMock.expectLastCall();
            return null;
        }
    }

    private Map<Map<String, Object>, Map<String, Object>> extractOffsets(List<List<SourceRecord>> recordLists) {
        Map<Map<String, Object>, Map<String, Object>> extractedOffsets = new HashMap<>();
        for (List<SourceRecord> records : recordLists) {
            for (SourceRecord record : records) {
                extractedOffsets.put((Map<String, Object>) record.sourcePartition(), (Map<String, Object>) record.sourceOffset());
            }
        }
        return extractedOffsets;
    }

    private List<SourceRecord> concatWithSelf(List<SourceRecord> listToConcat, int noConcats) {
        List<SourceRecord> result = new ArrayList<>(noConcats * listToConcat.size());
        for (int i = 0; i < noConcats; i++) {
            result.addAll(listToConcat);
        }
        return result;
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
        assertTrue(pollBatchTimeAvg >= 0.0d);
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
