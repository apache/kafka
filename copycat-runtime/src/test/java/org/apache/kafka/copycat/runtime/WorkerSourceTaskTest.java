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

package org.apache.kafka.copycat.runtime;

import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.copycat.cli.WorkerConfig;
import org.apache.kafka.copycat.source.SourceRecord;
import org.apache.kafka.copycat.source.SourceTask;
import org.apache.kafka.copycat.source.SourceTaskContext;
import org.apache.kafka.copycat.storage.Converter;
import org.apache.kafka.copycat.storage.OffsetStorageReader;
import org.apache.kafka.copycat.storage.OffsetStorageWriter;
import org.apache.kafka.copycat.util.Callback;
import org.apache.kafka.copycat.util.ConnectorTaskId;
import org.apache.kafka.copycat.util.ThreadedTest;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
public class WorkerSourceTaskTest extends ThreadedTest {
    private static final byte[] PARTITION_BYTES = "partition".getBytes();
    private static final byte[] OFFSET_BYTES = "offset-1".getBytes();

    // Copycat-format data
    private static final Integer KEY = -1;
    private static final Long RECORD = 12L;
    // Native-formatted data. The actual format of this data doesn't matter -- we just want to see that the right version
    // is used in the right place.
    private static final ByteBuffer CONVERTED_KEY = ByteBuffer.wrap("converted-key".getBytes());
    private static final String CONVERTED_RECORD = "converted-record";

    private ConnectorTaskId taskId = new ConnectorTaskId("job", 0);
    private WorkerConfig config;
    @Mock private SourceTask sourceTask;
    @Mock private Converter<ByteBuffer> keyConverter;
    @Mock private Converter<String> valueConverter;
    @Mock private KafkaProducer<ByteBuffer, String> producer;
    @Mock private OffsetStorageReader offsetReader;
    @Mock private OffsetStorageWriter<ByteBuffer, String> offsetWriter;
    private WorkerSourceTask<ByteBuffer, String> workerTask;
    @Mock private Future<RecordMetadata> sendFuture;

    private Capture<org.apache.kafka.clients.producer.Callback> producerCallbacks;

    private static final Properties EMPTY_TASK_PROPS = new Properties();
    private static final List<SourceRecord> RECORDS = Arrays.asList(
            new SourceRecord(PARTITION_BYTES, OFFSET_BYTES, "topic", null, KEY, RECORD)
    );

    @Override
    public void setup() {
        super.setup();
        Properties workerProps = new Properties();
        workerProps.setProperty("key.converter", "org.apache.kafka.copycat.json.JsonConverter");
        workerProps.setProperty("value.converter", "org.apache.kafka.copycat.json.JsonConverter");
        workerProps.setProperty("key.serializer", "org.apache.kafka.copycat.json.JsonSerializer");
        workerProps.setProperty("value.serializer", "org.apache.kafka.copycat.json.JsonSerializer");
        workerProps.setProperty("key.deserializer", "org.apache.kafka.copycat.json.JsonDeserializer");
        workerProps.setProperty("value.deserializer", "org.apache.kafka.copycat.json.JsonDeserializer");
        config = new WorkerConfig(workerProps);
        producerCallbacks = EasyMock.newCapture();
    }

    private void createWorkerTask() {
        workerTask = new WorkerSourceTask<>(taskId, sourceTask, keyConverter, valueConverter, producer,
                offsetReader, offsetWriter, config, new SystemTime());
    }

    @Test
    public void testPollsInBackground() throws Exception {
        createWorkerTask();

        sourceTask.initialize(EasyMock.anyObject(SourceTaskContext.class));
        EasyMock.expectLastCall();
        sourceTask.start(EMPTY_TASK_PROPS);
        EasyMock.expectLastCall();

        final CountDownLatch pollLatch = expectPolls(10);
        // In this test, we don't flush, so nothing goes any further than the offset writer

        sourceTask.stop();
        EasyMock.expectLastCall();
        expectOffsetFlush(true);

        PowerMock.replayAll();

        workerTask.start(EMPTY_TASK_PROPS);
        awaitPolls(pollLatch);
        workerTask.stop();
        assertEquals(true, workerTask.awaitStop(1000));

        PowerMock.verifyAll();
    }

    @Test
    public void testCommit() throws Exception {
        // Test that the task commits properly when prompted
        createWorkerTask();

        sourceTask.initialize(EasyMock.anyObject(SourceTaskContext.class));
        EasyMock.expectLastCall();
        sourceTask.start(EMPTY_TASK_PROPS);
        EasyMock.expectLastCall();

        // We'll wait for some data, then trigger a flush
        final CountDownLatch pollLatch = expectPolls(1);
        expectOffsetFlush(true);

        sourceTask.stop();
        EasyMock.expectLastCall();
        expectOffsetFlush(true);

        PowerMock.replayAll();

        workerTask.start(EMPTY_TASK_PROPS);
        awaitPolls(pollLatch);
        assertTrue(workerTask.commitOffsets());
        workerTask.stop();
        assertEquals(true, workerTask.awaitStop(1000));

        PowerMock.verifyAll();
    }

    @Test
    public void testCommitFailure() throws Exception {
        // Test that the task commits properly when prompted
        createWorkerTask();

        sourceTask.initialize(EasyMock.anyObject(SourceTaskContext.class));
        EasyMock.expectLastCall();
        sourceTask.start(EMPTY_TASK_PROPS);
        EasyMock.expectLastCall();

        // We'll wait for some data, then trigger a flush
        final CountDownLatch pollLatch = expectPolls(1);
        expectOffsetFlush(false);

        sourceTask.stop();
        EasyMock.expectLastCall();
        expectOffsetFlush(true);

        PowerMock.replayAll();

        workerTask.start(EMPTY_TASK_PROPS);
        awaitPolls(pollLatch);
        assertFalse(workerTask.commitOffsets());
        workerTask.stop();
        assertEquals(true, workerTask.awaitStop(1000));

        PowerMock.verifyAll();
    }

    @Test
    public void testSendRecordsConvertsData() throws Exception {
        createWorkerTask();

        List<SourceRecord> records = new ArrayList<>();
        // Can just use the same record for key and value
        records.add(new SourceRecord(PARTITION_BYTES, OFFSET_BYTES, "topic", null, KEY, RECORD));

        Capture<ProducerRecord<ByteBuffer, String>> sent = expectSendRecord();

        PowerMock.replayAll();

        Whitebox.invokeMethod(workerTask, "sendRecords", records);
        assertEquals(CONVERTED_KEY, sent.getValue().key());
        assertEquals(CONVERTED_RECORD, sent.getValue().value());

        PowerMock.verifyAll();
    }


    private CountDownLatch expectPolls(int count) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(count);
        // Note that we stub these to allow any number of calls because the thread will continue to
        // run. The count passed in + latch returned just makes sure we get *at least* that number of
        // calls
        EasyMock.expect(sourceTask.poll())
                .andStubAnswer(new IAnswer<List<SourceRecord>>() {
                    @Override
                    public List<SourceRecord> answer() throws Throwable {
                        latch.countDown();
                        return RECORDS;
                    }
                });
        // Fallout of the poll() call
        expectSendRecord();
        return latch;
    }

    private Capture<ProducerRecord<ByteBuffer, String>> expectSendRecord() throws InterruptedException {
        EasyMock.expect(keyConverter.fromCopycatData(KEY)).andStubReturn(CONVERTED_KEY);
        EasyMock.expect(valueConverter.fromCopycatData(RECORD)).andStubReturn(CONVERTED_RECORD);

        Capture<ProducerRecord<ByteBuffer, String>> sent = EasyMock.newCapture();
        // 1. Converted data passed to the producer, which will need callbacks invoked for flush to work
        EasyMock.expect(
                producer.send(EasyMock.capture(sent),
                        EasyMock.capture(producerCallbacks)))
                .andStubAnswer(new IAnswer<Future<RecordMetadata>>() {
                    @Override
                    public Future<RecordMetadata> answer() throws Throwable {
                        synchronized (producerCallbacks) {
                            for (org.apache.kafka.clients.producer.Callback cb : producerCallbacks.getValues()) {
                                cb.onCompletion(new RecordMetadata(new TopicPartition("foo", 0), 0, 0), null);
                            }
                            producerCallbacks.reset();
                        }
                        return sendFuture;
                    }
                });
        // 2. Offset data is passed to the offset storage.
        offsetWriter.setOffset(PARTITION_BYTES, OFFSET_BYTES);
        PowerMock.expectLastCall().anyTimes();

        return sent;
    }

    private void awaitPolls(CountDownLatch latch) throws InterruptedException {
        latch.await(1000, TimeUnit.MILLISECONDS);
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
            futureGetExpect.andReturn(null);
        } else {
            futureGetExpect.andThrow(new TimeoutException());
            offsetWriter.cancelFlush();
            PowerMock.expectLastCall();
        }
    }

}
