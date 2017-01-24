/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.ThreadedTest;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.api.easymock.annotation.MockStrict;
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
public class WorkerSourceTaskTest extends ThreadedTest {
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

    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private ConnectorTaskId taskId = new ConnectorTaskId("job", 0);
    private WorkerConfig config;
    @Mock private SourceTask sourceTask;
    @Mock private Converter keyConverter;
    @Mock private Converter valueConverter;
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
        config = new StandaloneConfig(workerProps);
        producerCallbacks = EasyMock.newCapture();
    }

    private void createWorkerTask() {
        createWorkerTask(TargetState.STARTED);
    }

    private void createWorkerTask(TargetState initialState) {
        workerTask = new WorkerSourceTask(taskId, sourceTask, statusListener, initialState, keyConverter, valueConverter, transformationChain,
                producer, offsetReader, offsetWriter, config, Time.SYSTEM);
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

        sourceTask.initialize(EasyMock.anyObject(SourceTaskContext.class));
        EasyMock.expectLastCall();
        sourceTask.start(TASK_PROPS);
        EasyMock.expectLastCall();
        statusListener.onStartup(taskId);
        EasyMock.expectLastCall();

        final CountDownLatch pollLatch = expectPolls(10);
        // In this test, we don't flush, so nothing goes any further than the offset writer

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

        assertTrue(awaitLatch(pollLatch));
        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();

        PowerMock.verifyAll();
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
        expectOffsetFlush(true);

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

        PowerMock.verifyAll();
    }

    @Test
    public void testCommit() throws Exception {
        // Test that the task commits properly when prompted
        createWorkerTask();

        sourceTask.initialize(EasyMock.anyObject(SourceTaskContext.class));
        EasyMock.expectLastCall();
        sourceTask.start(TASK_PROPS);
        EasyMock.expectLastCall();
        statusListener.onStartup(taskId);
        EasyMock.expectLastCall();

        // We'll wait for some data, then trigger a flush
        final CountDownLatch pollLatch = expectPolls(1);
        expectOffsetFlush(true);

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

        assertTrue(awaitLatch(pollLatch));
        assertTrue(workerTask.commitOffsets());
        workerTask.stop();
        assertTrue(workerTask.awaitStop(1000));

        taskFuture.get();

        PowerMock.verifyAll();
    }

    @Test
    public void testCommitFailure() throws Exception {
        // Test that the task commits properly when prompted
        createWorkerTask();

        sourceTask.initialize(EasyMock.anyObject(SourceTaskContext.class));
        EasyMock.expectLastCall();
        sourceTask.start(TASK_PROPS);
        EasyMock.expectLastCall();
        statusListener.onStartup(taskId);
        EasyMock.expectLastCall();

        // We'll wait for some data, then trigger a flush
        final CountDownLatch pollLatch = expectPolls(1);
        expectOffsetFlush(true);

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

        PowerMock.verifyAll();
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
    public void testSendRecordsTaskCommitRecordFail() throws Exception {
        createWorkerTask();

        // Differentiate only by Kafka partition so we can reuse conversion expectations
        SourceRecord record1 = new SourceRecord(PARTITION, OFFSET, "topic", 1, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record2 = new SourceRecord(PARTITION, OFFSET, "topic", 2, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);
        SourceRecord record3 = new SourceRecord(PARTITION, OFFSET, "topic", 3, KEY_SCHEMA, KEY, RECORD_SCHEMA, RECORD);

        // Source task commit record failure will not cause the task to abort
        expectSendRecordOnce(false);
        expectSendRecordTaskCommitRecordFail(false, false);
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
        expectOffsetFlush(true);

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
                        return RECORDS;
                    }
                });
        // Fallout of the poll() call
        expectSendRecordAnyTimes();
        return latch;
    }

    private CountDownLatch expectPolls(int count) throws InterruptedException {
        return expectPolls(count, new AtomicInteger());
    }

    @SuppressWarnings("unchecked")
    private void expectSendRecordSyncFailure(Throwable error) throws InterruptedException {
        expectConvertKeyValue(false);
        expectApplyTransformationChain(false);

        offsetWriter.offset(PARTITION, OFFSET);
        PowerMock.expectLastCall();

        EasyMock.expect(
                producer.send(EasyMock.anyObject(ProducerRecord.class),
                        EasyMock.anyObject(org.apache.kafka.clients.producer.Callback.class)))
                .andThrow(error);
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecordAnyTimes() throws InterruptedException {
        return expectSendRecordTaskCommitRecordSucceed(true, false);
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecordOnce(boolean isRetry) throws InterruptedException {
        return expectSendRecordTaskCommitRecordSucceed(false, isRetry);
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecordTaskCommitRecordSucceed(boolean anyTimes, boolean isRetry) throws InterruptedException {
        return expectSendRecord(anyTimes, isRetry, true);
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecordTaskCommitRecordFail(boolean anyTimes, boolean isRetry) throws InterruptedException {
        return expectSendRecord(anyTimes, isRetry, false);
    }

    private Capture<ProducerRecord<byte[], byte[]>> expectSendRecord(boolean anyTimes, boolean isRetry, boolean succeed) throws InterruptedException {
        expectConvertKeyValue(anyTimes);
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

        // 3. As a result of a successful producer send callback, we'll notify the source task of the record commit
        expectTaskCommitRecord(anyTimes, succeed);

        return sent;
    }

    private void expectConvertKeyValue(boolean anyTimes) {
        IExpectationSetters<byte[]> convertKeyExpect = EasyMock.expect(keyConverter.fromConnectData(TOPIC, KEY_SCHEMA, KEY));
        if (anyTimes)
            convertKeyExpect.andStubReturn(SERIALIZED_KEY);
        else
            convertKeyExpect.andReturn(SERIALIZED_KEY);
        IExpectationSetters<byte[]> convertValueExpect = EasyMock.expect(valueConverter.fromConnectData(TOPIC, RECORD_SCHEMA, RECORD));
        if (anyTimes)
            convertValueExpect.andStubReturn(SERIALIZED_RECORD);
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

    private void expectTaskCommitRecord(boolean anyTimes, boolean succeed) throws InterruptedException {
        sourceTask.commitRecord(EasyMock.anyObject(SourceRecord.class));
        IExpectationSetters<Void> expect = EasyMock.expectLastCall();
        if (!succeed) {
            expect = expect.andThrow(new RuntimeException("Error committing record in source task"));
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

    private abstract static class TestSourceTask extends SourceTask {
    }

}
