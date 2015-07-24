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
import org.apache.kafka.copycat.data.GenericRecord;
import org.apache.kafka.copycat.data.GenericRecordBuilder;
import org.apache.kafka.copycat.data.Schema;
import org.apache.kafka.copycat.data.SchemaBuilder;
import org.apache.kafka.copycat.errors.CopycatRuntimeException;
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
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.*;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest(WorkerSinkTask.class)
@PowerMockIgnore("javax.management.*")
public class WorkerSinkTaskTest extends ThreadedTest {

    // These are fixed to keep this code simpler
    private static final String TOPIC = "test";
    private static final int PARTITION = 12;
    private static final long FIRST_OFFSET = 45;
    private static final String KEY = "KEY";
    private static final String VALUE = "VALUE";
    private static final String TOPIC_PARTITION_STR = "test-12";

    private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, PARTITION);
    private static final org.apache.kafka.copycat.connector.TopicPartition TOPIC_PARTITION_COPYCAT =
            new org.apache.kafka.copycat.connector.TopicPartition(TOPIC, PARTITION);

    private ConnectorTaskId taskId = new ConnectorTaskId("job", 0);
    private Time time;
    private SinkTask sinkTask;
    private WorkerConfig workerConfig;
    private Converter converter;
    private WorkerSinkTask workerTask;
    private KafkaConsumer<Object, Object> consumer;
    private WorkerSinkTaskThread workerThread;

    private long recordsReturned;

    @Override
    public void setup() {
        super.setup();
        time = new MockTime();
        sinkTask = PowerMock.createMock(SinkTask.class);
        workerConfig = new WorkerConfig();
        converter = PowerMock.createMock(Converter.class);
        workerTask = PowerMock.createPartialMock(
                WorkerSinkTask.class, new String[]{"createConsumer", "createWorkerThread"},
                taskId, sinkTask, workerConfig, converter, time);

        recordsReturned = 0;
    }

    @Test
    public void testGetInputTopicPartitions() throws Exception {
        Properties props = new Properties();
        props.setProperty(SinkTask.TOPICPARTITIONS_CONFIG, "topic-1,foo-2");
        assertEquals(
                Arrays.asList(new org.apache.kafka.copycat.connector.TopicPartition("topic", 1),
                        new org.apache.kafka.copycat.connector.TopicPartition("foo", 2)),
                Whitebox.invokeMethod(workerTask, "getInputTopicPartitions", props)
        );
    }

    @Test
    public void testPollsInBackground() throws Exception {
        Properties taskProps = new Properties();
        taskProps.setProperty(SinkTask.TOPICPARTITIONS_CONFIG, TOPIC_PARTITION_STR);

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
                        = new SinkRecord(TOPIC, PARTITION, KEY, VALUE, FIRST_OFFSET + offset);
                assertEquals(referenceSinkRecord, rec);
                offset++;
            }
        }

        PowerMock.verifyAll();
    }

    @Test
    public void testDeliverConvertsData() throws Exception {
        // Validate conversion is performed when data is delivered
        Schema schema = SchemaBuilder.record("sample").fields().endRecord();
        GenericRecord record = new GenericRecordBuilder(schema).build();
        byte[] rawKey = "key".getBytes(), rawValue = "value".getBytes();

        ConsumerRecords<Object, Object> records = new ConsumerRecords<Object, Object>(
                Collections.singletonMap(
                        new TopicPartition("topic", 0),
                        Collections.singletonList(
                                new ConsumerRecord<Object, Object>("topic", 0, 0, rawKey, rawValue))));

        // Exact data doesn't matter, but should be passed directly to sink task
        EasyMock.expect(converter.toCopycatData(rawKey))
                .andReturn(record);
        EasyMock.expect(converter.toCopycatData(rawValue))
                .andReturn(record);
        Capture<Collection<SinkRecord>> capturedRecords
                = EasyMock.newCapture(CaptureType.ALL);
        sinkTask.put(EasyMock.capture(capturedRecords));
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        Whitebox.invokeMethod(workerTask, "deliverMessages", records);
        assertEquals(record, capturedRecords.getValue().iterator().next().getKey());
        assertEquals(record, capturedRecords.getValue().iterator().next().getValue());

        PowerMock.verifyAll();
    }

    @Test
    public void testCommit() throws Exception {
        Properties taskProps = new Properties();
        taskProps.setProperty(SinkTask.TOPICPARTITIONS_CONFIG, TOPIC_PARTITION_STR);

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
        assertEquals(0, workerThread.getCommitFailures());
        workerTask.stop();
        workerTask.close();

        assertEquals(2, capturedRecords.getValues().size());

        PowerMock.verifyAll();
    }

    @Test
    public void testCommitTaskFlushFailure() throws Exception {
        Properties taskProps = new Properties();
        taskProps.setProperty(SinkTask.TOPICPARTITIONS_CONFIG, TOPIC_PARTITION_STR);

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
        assertEquals(1, workerThread.getCommitFailures());
        assertEquals(false, Whitebox.getInternalState(workerThread, "committing"));
        workerTask.stop();
        workerTask.close();

        PowerMock.verifyAll();
    }

    @Test
    public void testCommitConsumerFailure() throws Exception {
        Properties taskProps = new Properties();
        taskProps.setProperty(SinkTask.TOPICPARTITIONS_CONFIG, TOPIC_PARTITION_STR);

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
        assertEquals(1, workerThread.getCommitFailures());
        assertEquals(false, Whitebox.getInternalState(workerThread, "committing"));
        workerTask.stop();
        workerTask.close();

        PowerMock.verifyAll();
    }

    @Test
    public void testCommitTimeout() throws Exception {
        Properties taskProps = new Properties();
        taskProps.setProperty(SinkTask.TOPICPARTITIONS_CONFIG, TOPIC_PARTITION_STR);

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
        assertEquals(1, workerThread.getCommitFailures());
        assertEquals(false, Whitebox.getInternalState(workerThread, "committing"));
        workerTask.stop();
        workerTask.close();

        PowerMock.verifyAll();
    }

    @Test
    public void testGetInputPartitionsSingle() throws Exception {
        Properties taskProps = new Properties();
        taskProps.setProperty(SinkTask.TOPICPARTITIONS_CONFIG, "test-1");

        assertEquals(Arrays.asList(new org.apache.kafka.copycat.connector.TopicPartition("test", 1)),
                Whitebox.invokeMethod(workerTask, "getInputTopicPartitions", taskProps));
    }

    @Test
    public void testGetInputPartitionsList() throws Exception {
        Properties taskProps = new Properties();
        taskProps.setProperty(SinkTask.TOPICPARTITIONS_CONFIG, "test-1,foo-2,bar-3");

        assertEquals(Arrays.asList(
                        new org.apache.kafka.copycat.connector.TopicPartition("test", 1),
                        new org.apache.kafka.copycat.connector.TopicPartition("foo", 2),
                        new org.apache.kafka.copycat.connector.TopicPartition("bar", 3)),
                Whitebox.invokeMethod(workerTask, "getInputTopicPartitions", taskProps));
    }

    @Test(expected = CopycatRuntimeException.class)
    public void testGetInputPartitionsMissing() throws Exception {
        // Missing setting
        Whitebox.invokeMethod(workerTask, "getInputTopicPartitions", new Properties());
    }


    private KafkaConsumer<Object, Object> expectInitializeTask(Properties taskProps)
            throws Exception {
        sinkTask.initialize(EasyMock.anyObject(SinkTaskContext.class));
        PowerMock.expectLastCall();
        sinkTask.start(taskProps);
        PowerMock.expectLastCall();

        consumer = PowerMock.createMock(KafkaConsumer.class);
        PowerMock.expectPrivate(workerTask, "createConsumer", taskProps)
                .andReturn(consumer);
        workerThread = PowerMock.createPartialMock(WorkerSinkTaskThread.class, new String[]{"start"},
                workerTask, "mock-worker-thread", time,
                workerConfig);
        PowerMock.expectPrivate(workerTask, "createWorkerThread")
                .andReturn(workerThread);
        workerThread.start();
        PowerMock.expectLastCall();
        return consumer;
    }

    private void expectStopTask(final long expectedMessages) throws Exception {
        final long finalOffset = FIRST_OFFSET + expectedMessages - 1;

        sinkTask.stop();
        PowerMock.expectLastCall();

        // Triggers final offset commit
        EasyMock.expect(consumer.subscriptions()).andReturn(Collections.singleton(TOPIC_PARTITION));
        EasyMock.expect(consumer.position(TOPIC_PARTITION)).andAnswer(new IAnswer<Long>() {
            @Override
            public Long answer() throws Throwable {
                return FIRST_OFFSET + recordsReturned - 1;
            }
        });
        final Capture<ConsumerCommitCallback> capturedCallback = EasyMock.newCapture();
        consumer.commit(EasyMock.eq(Collections.singletonMap(TOPIC_PARTITION, finalOffset)),
                EasyMock.eq(CommitType.SYNC),
                EasyMock.capture(capturedCallback));

        consumer.close();
        PowerMock.expectLastCall();
    }

    // Note that this can only be called once per test currently
    private Capture<Collection<SinkRecord>> expectPolls(final long pollDelayMs) throws Exception {
        // Stub out all the consumer stream/iterator responses, which we just want to verify occur,
        // but don't care about the exact details here.
        EasyMock.expect(consumer.poll(EasyMock.anyLong())).andStubAnswer(
                new IAnswer<ConsumerRecords<Object, Object>>() {
                    @Override
                    public ConsumerRecords<Object, Object> answer() throws Throwable {
                        // "Sleep" so time will progress
                        time.sleep(pollDelayMs);
                        ConsumerRecords<Object, Object> records = new ConsumerRecords<Object, Object>(
                                Collections.singletonMap(new TopicPartition(TOPIC, PARTITION), Arrays.asList(
                                        new ConsumerRecord<Object, Object>(TOPIC, PARTITION,
                                                FIRST_OFFSET + recordsReturned, KEY,
                                                VALUE))));
                        recordsReturned++;
                        return records;
                    }
                });
        EasyMock.expect(converter.toCopycatData(KEY)).andReturn(KEY).anyTimes();
        EasyMock.expect(converter.toCopycatData(VALUE)).andReturn(VALUE).anyTimes();
        Capture<Collection<SinkRecord>> capturedRecords = EasyMock.newCapture(CaptureType.ALL);
        sinkTask.put(EasyMock.capture(capturedRecords));
        EasyMock.expectLastCall().anyTimes();
        return capturedRecords;
    }

    private Capture<ConsumerCommitCallback> expectOffsetFlush(final long expectedMessages,
                                                              final RuntimeException flushError,
                                                              final Exception consumerCommitError,
                                                              final long consumerCommitDelayMs,
                                                              final boolean invokeCallback)
            throws Exception {
        final long finalOffset = FIRST_OFFSET + expectedMessages - 1;

        EasyMock.expect(consumer.subscriptions()).andReturn(Collections.singleton(TOPIC_PARTITION));
        EasyMock.expect(consumer.position(TOPIC_PARTITION)).andAnswer(
                new IAnswer<Long>() {
                    @Override
                    public Long answer() throws Throwable {
                        return FIRST_OFFSET + recordsReturned - 1;
                    }
                }
        );

        sinkTask.flush(Collections.singletonMap(TOPIC_PARTITION_COPYCAT, finalOffset));
        IExpectationSetters<Object> flushExpectation = PowerMock.expectLastCall();
        if (flushError != null) {
            flushExpectation.andThrow(flushError).once();
            return null;
        }

        final Capture<ConsumerCommitCallback> capturedCallback = EasyMock.newCapture();
        final Map<TopicPartition, Long> offsets = Collections.singletonMap(TOPIC_PARTITION, finalOffset);
        consumer.commit(EasyMock.eq(offsets),
                EasyMock.eq(CommitType.ASYNC),
                EasyMock.capture(capturedCallback));
        PowerMock.expectLastCall().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                time.sleep(consumerCommitDelayMs);
                if (invokeCallback) {
                    capturedCallback.getValue().onComplete(offsets, consumerCommitError);
                }
                return null;
            }
        });
        return capturedCallback;
    }

}
