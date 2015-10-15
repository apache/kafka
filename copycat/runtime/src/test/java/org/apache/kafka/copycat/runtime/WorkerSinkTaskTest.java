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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.copycat.cli.WorkerConfig;
import org.apache.kafka.copycat.data.Schema;
import org.apache.kafka.copycat.data.SchemaAndValue;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.sink.SinkRecord;
import org.apache.kafka.copycat.sink.SinkTask;
import org.apache.kafka.copycat.sink.SinkTaskContext;
import org.apache.kafka.copycat.storage.Converter;
import org.apache.kafka.copycat.util.ConnectorTaskId;
import org.apache.kafka.copycat.util.MockTime;
import org.apache.kafka.copycat.util.ThreadedTest;
import org.easymock.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@PrepareForTest(WorkerSinkTask.class)
@PowerMockIgnore("javax.management.*")
public class WorkerSinkTaskTest extends ThreadedTest {

    // These are fixed to keep this code simpler. In this example we assume byte[] raw values
    // with mix of integer/string in Copycat
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
    private static final TopicPartition UNASSIGNED_TOPIC_PARTITION = new TopicPartition(TOPIC, 200);

    private ConnectorTaskId taskId = new ConnectorTaskId("job", 0);
    private Time time;
    @Mock private SinkTask sinkTask;
    private Capture<SinkTaskContext> sinkTaskContext = EasyMock.newCapture();
    private WorkerConfig workerConfig;
    @Mock private Converter keyConverter;
    @Mock
    private Converter valueConverter;
    private WorkerSinkTask workerTask;
    @Mock private KafkaConsumer<byte[], byte[]> consumer;
    private WorkerSinkTaskThread workerThread;

    private long recordsReturned;

    @SuppressWarnings("unchecked")
    @Override
    public void setup() {
        super.setup();
        time = new MockTime();
        Properties workerProps = new Properties();
        workerProps.setProperty("key.converter", "org.apache.kafka.copycat.json.JsonConverter");
        workerProps.setProperty("value.converter", "org.apache.kafka.copycat.json.JsonConverter");
        workerProps.setProperty("internal.key.converter", "org.apache.kafka.copycat.json.JsonConverter");
        workerProps.setProperty("internal.value.converter", "org.apache.kafka.copycat.json.JsonConverter");
        workerProps.setProperty("internal.key.converter.schemas.enable", "false");
        workerProps.setProperty("internal.value.converter.schemas.enable", "false");
        workerConfig = new WorkerConfig(workerProps);
        workerTask = PowerMock.createPartialMock(
                WorkerSinkTask.class, new String[]{"createConsumer", "createWorkerThread"},
                taskId, sinkTask, workerConfig, keyConverter, valueConverter, time);

        recordsReturned = 0;
    }

    @Test
    public void testPollsInBackground() throws Exception {
        Properties taskProps = new Properties();

        expectInitializeTask(taskProps);
        Capture<Collection<SinkRecord>> capturedRecords = expectPolls(1L);
        expectStopTask(10L);

        PowerMock.replayAll();

        workerTask.start(taskProps);
        for (int i = 0; i < 10; i++) {
            workerThread.iteration();
        }
        workerTask.stop();
        // No need for awaitStop since the thread is mocked
        workerTask.close();

        // Verify contents match expected values, i.e. that they were translated properly. With max
        // batch size 1 and poll returns 1 message at a time, we should have a matching # of batches
        assertEquals(10, capturedRecords.getValues().size());
        int offset = 0;
        for (Collection<SinkRecord> recs : capturedRecords.getValues()) {
            assertEquals(1, recs.size());
            for (SinkRecord rec : recs) {
                SinkRecord referenceSinkRecord
                        = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, KEY, VALUE_SCHEMA, VALUE, FIRST_OFFSET + offset);
                assertEquals(referenceSinkRecord, rec);
                offset++;
            }
        }

        PowerMock.verifyAll();
    }

    @Test
    public void testDeliverConvertsData() throws Exception {
        // Validate conversion is performed when data is delivered
        SchemaAndValue record = new SchemaAndValue(Schema.INT32_SCHEMA, 12);

        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(
                Collections.singletonMap(
                        new TopicPartition(TOPIC, 0),
                        Collections.singletonList(new ConsumerRecord<>(TOPIC, 0, 0, RAW_KEY, RAW_VALUE))));

        // Exact data doesn't matter, but should be passed directly to sink task
        EasyMock.expect(keyConverter.toCopycatData(EasyMock.eq(TOPIC), EasyMock.aryEq(RAW_KEY))).andReturn(record);
        EasyMock.expect(valueConverter.toCopycatData(EasyMock.eq(TOPIC), EasyMock.aryEq(RAW_VALUE))).andReturn(record);
        Capture<Collection<SinkRecord>> capturedRecords
                = EasyMock.newCapture(CaptureType.ALL);
        sinkTask.put(EasyMock.capture(capturedRecords));
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        Whitebox.invokeMethod(workerTask, "deliverMessages", records);
        assertEquals(record.schema(), capturedRecords.getValue().iterator().next().keySchema());
        assertEquals(record.value(), capturedRecords.getValue().iterator().next().key());
        assertEquals(record.schema(), capturedRecords.getValue().iterator().next().valueSchema());
        assertEquals(record.value(), capturedRecords.getValue().iterator().next().value());

        PowerMock.verifyAll();
    }

    @Test
    public void testCommit() throws Exception {
        Properties taskProps = new Properties();

        expectInitializeTask(taskProps);
        // Make each poll() take the offset commit interval
        Capture<Collection<SinkRecord>> capturedRecords
                = expectPolls(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT);
        expectOffsetFlush(1L, null, null, 0, true);
        expectStopTask(2);

        PowerMock.replayAll();

        workerTask.start(taskProps);
        // First iteration gets one record
        workerThread.iteration();
        // Second triggers commit, gets a second offset
        workerThread.iteration();
        // Commit finishes synchronously for testing so we can check this immediately
        assertEquals(0, workerThread.commitFailures());
        workerTask.stop();
        workerTask.close();

        assertEquals(2, capturedRecords.getValues().size());

        PowerMock.verifyAll();
    }

    @Test
    public void testCommitTaskFlushFailure() throws Exception {
        Properties taskProps = new Properties();

        expectInitializeTask(taskProps);
        Capture<Collection<SinkRecord>> capturedRecords
                = expectPolls(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT);
        expectOffsetFlush(1L, new RuntimeException(), null, 0, true);
        expectStopTask(2);

        PowerMock.replayAll();

        workerTask.start(taskProps);
        // Second iteration triggers commit
        workerThread.iteration();
        workerThread.iteration();
        assertEquals(1, workerThread.commitFailures());
        assertEquals(false, Whitebox.getInternalState(workerThread, "committing"));
        workerTask.stop();
        workerTask.close();

        PowerMock.verifyAll();
    }

    @Test
    public void testCommitConsumerFailure() throws Exception {
        Properties taskProps = new Properties();

        expectInitializeTask(taskProps);
        Capture<Collection<SinkRecord>> capturedRecords
                = expectPolls(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT);
        expectOffsetFlush(1L, null, new Exception(), 0, true);
        expectStopTask(2);

        PowerMock.replayAll();

        workerTask.start(taskProps);
        // Second iteration triggers commit
        workerThread.iteration();
        workerThread.iteration();
        // TODO Response to consistent failures?
        assertEquals(1, workerThread.commitFailures());
        assertEquals(false, Whitebox.getInternalState(workerThread, "committing"));
        workerTask.stop();
        workerTask.close();

        PowerMock.verifyAll();
    }

    @Test
    public void testCommitTimeout() throws Exception {
        Properties taskProps = new Properties();

        expectInitializeTask(taskProps);
        // Cut down amount of time to pass in each poll so we trigger exactly 1 offset commit
        Capture<Collection<SinkRecord>> capturedRecords
                = expectPolls(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT / 2);
        expectOffsetFlush(2L, null, null, WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_DEFAULT, false);
        expectStopTask(4);

        PowerMock.replayAll();

        workerTask.start(taskProps);
        // Third iteration triggers commit, fourth gives a chance to trigger the timeout but doesn't
        // trigger another commit
        workerThread.iteration();
        workerThread.iteration();
        workerThread.iteration();
        workerThread.iteration();
        // TODO Response to consistent failures?
        assertEquals(1, workerThread.commitFailures());
        assertEquals(false, Whitebox.getInternalState(workerThread, "committing"));
        workerTask.stop();
        workerTask.close();

        PowerMock.verifyAll();
    }

    @Test
    public void testAssignmentPauseResume() throws Exception {
        // Just validate that the calls are passed through to the consumer, and that where appropriate errors are
        // converted

        Properties taskProps = new Properties();

        expectInitializeTask(taskProps);

        expectOnePoll().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                assertEquals(new HashSet<>(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2, TOPIC_PARTITION3)),
                        sinkTaskContext.getValue().assignment());
                return null;
            }
        });
        EasyMock.expect(consumer.assignment()).andReturn(new HashSet<>(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2, TOPIC_PARTITION3)));

        expectOnePoll().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                try {
                    sinkTaskContext.getValue().pause(UNASSIGNED_TOPIC_PARTITION);
                    fail("Trying to pause unassigned partition should have thrown an Copycat exception");
                } catch (CopycatException e) {
                    // expected
                }
                sinkTaskContext.getValue().pause(TOPIC_PARTITION, TOPIC_PARTITION2);
                return null;
            }
        });
        consumer.pause(UNASSIGNED_TOPIC_PARTITION);
        PowerMock.expectLastCall().andThrow(new IllegalStateException("unassigned topic partition"));
        consumer.pause(TOPIC_PARTITION, TOPIC_PARTITION2);
        PowerMock.expectLastCall();

        expectOnePoll().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                try {
                    sinkTaskContext.getValue().resume(UNASSIGNED_TOPIC_PARTITION);
                    fail("Trying to resume unassigned partition should have thrown an Copycat exception");
                } catch (CopycatException e) {
                    // expected
                }

                sinkTaskContext.getValue().resume(TOPIC_PARTITION, TOPIC_PARTITION2);
                return null;
            }
        });
        consumer.resume(UNASSIGNED_TOPIC_PARTITION);
        PowerMock.expectLastCall().andThrow(new IllegalStateException("unassigned topic partition"));
        consumer.resume(TOPIC_PARTITION, TOPIC_PARTITION2);
        PowerMock.expectLastCall();

        expectStopTask(0);

        PowerMock.replayAll();

        workerTask.start(taskProps);
        workerThread.iteration();
        workerThread.iteration();
        workerThread.iteration();
        workerTask.stop();
        workerTask.close();

        PowerMock.verifyAll();
    }


    private void expectInitializeTask(Properties taskProps) throws Exception {
        sinkTask.initialize(EasyMock.capture(sinkTaskContext));
        PowerMock.expectLastCall();
        sinkTask.start(taskProps);
        PowerMock.expectLastCall();

        PowerMock.expectPrivate(workerTask, "createConsumer", taskProps)
                .andReturn(consumer);
        workerThread = PowerMock.createPartialMock(WorkerSinkTaskThread.class, new String[]{"start"},
                workerTask, "mock-worker-thread", time,
                workerConfig);
        PowerMock.expectPrivate(workerTask, "createWorkerThread")
                .andReturn(workerThread);
        workerThread.start();
        PowerMock.expectLastCall();
    }

    private void expectStopTask(final long expectedMessages) throws Exception {
        final long finalOffset = FIRST_OFFSET + expectedMessages - 1;

        sinkTask.stop();
        PowerMock.expectLastCall();

        // No offset commit since it happens in the mocked worker thread, but the main thread does need to wake up the
        // consumer so it exits quickly
        consumer.wakeup();
        PowerMock.expectLastCall();

        consumer.close();
        PowerMock.expectLastCall();
    }

    // Note that this can only be called once per test currently
    private Capture<Collection<SinkRecord>> expectPolls(final long pollDelayMs) throws Exception {
        // Stub out all the consumer stream/iterator responses, which we just want to verify occur,
        // but don't care about the exact details here.
        EasyMock.expect(consumer.poll(EasyMock.anyLong())).andStubAnswer(
                new IAnswer<ConsumerRecords<byte[], byte[]>>() {
                    @Override
                    public ConsumerRecords<byte[], byte[]> answer() throws Throwable {
                        // "Sleep" so time will progress
                        time.sleep(pollDelayMs);
                        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(
                                Collections.singletonMap(
                                        new TopicPartition(TOPIC, PARTITION),
                                        Arrays.asList(
                                                new ConsumerRecord<>(TOPIC, PARTITION, FIRST_OFFSET + recordsReturned, RAW_KEY, RAW_VALUE)
                                        )));
                        recordsReturned++;
                        return records;
                    }
                });
        EasyMock.expect(keyConverter.toCopycatData(TOPIC, RAW_KEY)).andReturn(new SchemaAndValue(KEY_SCHEMA, KEY)).anyTimes();
        EasyMock.expect(valueConverter.toCopycatData(TOPIC, RAW_VALUE)).andReturn(new SchemaAndValue(VALUE_SCHEMA, VALUE)).anyTimes();
        Capture<Collection<SinkRecord>> capturedRecords = EasyMock.newCapture(CaptureType.ALL);
        sinkTask.put(EasyMock.capture(capturedRecords));
        EasyMock.expectLastCall().anyTimes();
        return capturedRecords;
    }

    private IExpectationSetters<Object> expectOnePoll() {
        // Currently the SinkTask's put() method will not be invoked unless we provide some data, so instead of
        // returning empty data, we return one record. The expectation is that the data will be ignored by the
        // response behavior specified using the return value of this method.
        EasyMock.expect(consumer.poll(EasyMock.anyLong())).andAnswer(
                new IAnswer<ConsumerRecords<byte[], byte[]>>() {
                    @Override
                    public ConsumerRecords<byte[], byte[]> answer() throws Throwable {
                        // "Sleep" so time will progress
                        time.sleep(1L);
                        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(
                                Collections.singletonMap(
                                        new TopicPartition(TOPIC, PARTITION),
                                        Arrays.asList(
                                                new ConsumerRecord<>(TOPIC, PARTITION, FIRST_OFFSET + recordsReturned, RAW_KEY, RAW_VALUE)
                                        )));
                        recordsReturned++;
                        return records;
                    }
                });
        EasyMock.expect(keyConverter.toCopycatData(TOPIC, RAW_KEY)).andReturn(new SchemaAndValue(KEY_SCHEMA, KEY));
        EasyMock.expect(valueConverter.toCopycatData(TOPIC, RAW_VALUE)).andReturn(new SchemaAndValue(VALUE_SCHEMA, VALUE));
        sinkTask.put(EasyMock.anyObject(Collection.class));
        return EasyMock.expectLastCall();
    }

    private Capture<OffsetCommitCallback> expectOffsetFlush(final long expectedMessages,
                                                              final RuntimeException flushError,
                                                              final Exception consumerCommitError,
                                                              final long consumerCommitDelayMs,
                                                              final boolean invokeCallback)
            throws Exception {
        final long finalOffset = FIRST_OFFSET + expectedMessages - 1;

        EasyMock.expect(consumer.assignment()).andReturn(Collections.singleton(TOPIC_PARTITION));
        EasyMock.expect(consumer.position(TOPIC_PARTITION)).andAnswer(
                new IAnswer<Long>() {
                    @Override
                    public Long answer() throws Throwable {
                        return FIRST_OFFSET + recordsReturned - 1;
                    }
                }
        );

        sinkTask.flush(Collections.singletonMap(TOPIC_PARTITION, new OffsetAndMetadata(finalOffset)));
        IExpectationSetters<Object> flushExpectation = PowerMock.expectLastCall();
        if (flushError != null) {
            flushExpectation.andThrow(flushError).once();
            return null;
        }

        final Capture<OffsetCommitCallback> capturedCallback = EasyMock.newCapture();
        final Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(TOPIC_PARTITION, new OffsetAndMetadata(finalOffset));
        consumer.commitAsync(EasyMock.eq(offsets),
                EasyMock.capture(capturedCallback));
        PowerMock.expectLastCall().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                time.sleep(consumerCommitDelayMs);
                if (invokeCallback)
                    capturedCallback.getValue().onComplete(offsets, consumerCommitError);
                return null;
            }
        });
        return capturedCallback;
    }

}
