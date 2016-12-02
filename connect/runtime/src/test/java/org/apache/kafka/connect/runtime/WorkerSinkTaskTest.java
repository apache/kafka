/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.MockTime;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
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
    private static final long FIRST_OFFSET = 45;
    private static final Schema KEY_SCHEMA = Schema.INT32_SCHEMA;
    private static final int KEY = 12;
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;
    private static final String VALUE = "VALUE";
    private static final byte[] RAW_KEY = "key".getBytes();
    private static final byte[] RAW_VALUE = "value".getBytes();

    private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, PARTITION);
    private static final TopicPartition TOPIC_PARTITION2 = new TopicPartition(TOPIC, PARTITION2);

    private static final Map<String, String> TASK_PROPS = new HashMap<>();
    static {
        TASK_PROPS.put(SinkConnector.TOPICS_CONFIG, TOPIC);
        TASK_PROPS.put(TaskConfig.TASK_CLASS_CONFIG, TestSinkTask.class.getName());
    }
    private static final TaskConfig TASK_CONFIG = new TaskConfig(TASK_PROPS);

    private ConnectorTaskId taskId = new ConnectorTaskId("job", 0);
    private TargetState initialState = TargetState.STARTED;
    private Time time;
    private WorkerSinkTask workerTask;
    @Mock
    private SinkTask sinkTask;
    private Capture<WorkerSinkTaskContext> sinkTaskContext = EasyMock.newCapture();
    private WorkerConfig workerConfig;
    @Mock
    private Converter keyConverter;
    @Mock
    private Converter valueConverter;
    @Mock
    private TaskStatus.Listener statusListener;
    @Mock
    private KafkaConsumer<byte[], byte[]> consumer;
    private Capture<ConsumerRebalanceListener> rebalanceListener = EasyMock.newCapture();

    private long recordsReturned;

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
        workerTask = PowerMock.createPartialMock(
                WorkerSinkTask.class, new String[]{"createConsumer"},
                taskId, sinkTask, statusListener, initialState, workerConfig, keyConverter, valueConverter, time);

        recordsReturned = 0;
    }

    @Test
    public void testStartPaused() throws Exception {
        workerTask = PowerMock.createPartialMock(
                WorkerSinkTask.class, new String[]{"createConsumer"},
                taskId, sinkTask, statusListener, TargetState.PAUSED, workerConfig, keyConverter, valueConverter, time);

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

        PowerMock.verifyAll();
    }

    @Test
    public void testPause() throws Exception {
        expectInitializeTask();
        expectPollInitialAssignment();

        expectConsumerPoll(1);
        expectConvertMessages(1);
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

        // No records returned
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
        expectConvertMessages(1);
        sinkTask.put(EasyMock.<Collection<SinkRecord>>anyObject());
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration(); // initial assignment
        workerTask.iteration(); // fetch some data
        workerTask.transitionTo(TargetState.PAUSED);
        workerTask.iteration(); // wakeup
        workerTask.iteration(); // now paused
        workerTask.transitionTo(TargetState.STARTED);
        workerTask.iteration(); // wakeup
        workerTask.iteration(); // now unpaused

        PowerMock.verifyAll();
    }

    @Test
    public void testPollRedelivery() throws Exception {
        expectInitializeTask();
        expectPollInitialAssignment();

        // If a retriable exception is thrown, we should redeliver the same batch, pausing the consumer in the meantime
        expectConsumerPoll(1);
        expectConvertMessages(1);
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
        workerTask.iteration();
        workerTask.iteration();

        PowerMock.verifyAll();
    }

    @Test
    public void testErrorInRebalancePartitionRevocation() throws Exception {
        RuntimeException exception = new RuntimeException("Revocation error");

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
        expectInitializeTask();
        expectPollInitialAssignment();

        final List<TopicPartition> partitions = asList(TOPIC_PARTITION, TOPIC_PARTITION2);

        sinkTask.close(new HashSet<>(partitions));
        EasyMock.expectLastCall();

        sinkTask.flush(EasyMock.<Map<TopicPartition, OffsetAndMetadata>>anyObject());
        EasyMock.expectLastCall();

        // first one raises wakeup
        consumer.commitSync(EasyMock.<Map<TopicPartition, OffsetAndMetadata>>anyObject());
        EasyMock.expectLastCall().andThrow(new WakeupException());

        // we should retry and complete the commit
        consumer.commitSync(EasyMock.<Map<TopicPartition, OffsetAndMetadata>>anyObject());
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

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration(); // poll for initial assignment
        workerTask.iteration(); // now rebalance with the wakeup triggered

        PowerMock.verifyAll();
    }

    @Test
    public void testMissingTimestampPropagation() throws Exception {
        expectInitializeTask();
        expectConsumerPoll(1, Record.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE);
        expectConvertMessages(1);

        Capture<Collection<SinkRecord>> records = EasyMock.newCapture(CaptureType.ALL);

        sinkTask.put(EasyMock.capture(records));

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration();

        SinkRecord record = records.getValue().iterator().next();

        // we expect null for missing timestamp, the sentinel value of Record.NO_TIMESTAMP is Kafka's API
        assertEquals(null, record.timestamp());
        assertEquals(TimestampType.NO_TIMESTAMP_TYPE, record.timestampType());

        PowerMock.verifyAll();
    }

    @Test
    public void testTimestampPropagation() throws Exception {
        final Long timestamp = System.currentTimeMillis();
        final TimestampType timestampType = TimestampType.CREATE_TIME;

        expectInitializeTask();
        expectConsumerPoll(1, timestamp, timestampType);
        expectConvertMessages(1);

        Capture<Collection<SinkRecord>> records = EasyMock.newCapture(CaptureType.ALL);

        sinkTask.put(EasyMock.capture(records));

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration();

        SinkRecord record = records.getValue().iterator().next();

        assertEquals(timestamp, record.timestamp());
        assertEquals(timestampType, record.timestampType());

        PowerMock.verifyAll();
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

        sinkTask.flush(EasyMock.<Map<TopicPartition, OffsetAndMetadata>>anyObject());
        EasyMock.expectLastCall();

        consumer.commitSync(EasyMock.<Map<TopicPartition, OffsetAndMetadata>>anyObject());
        EasyMock.expectLastCall();

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
        expectConsumerPoll(numMessages, Record.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE);
    }

    private void expectConsumerPoll(final int numMessages, final long timestamp, final TimestampType timestampType) {
        EasyMock.expect(consumer.poll(EasyMock.anyLong())).andAnswer(
                new IAnswer<ConsumerRecords<byte[], byte[]>>() {
                    @Override
                    public ConsumerRecords<byte[], byte[]> answer() throws Throwable {
                        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
                        for (int i = 0; i < numMessages; i++)
                            records.add(new ConsumerRecord<>(TOPIC, PARTITION, FIRST_OFFSET + recordsReturned + i, timestamp, timestampType, 0L, 0, 0, RAW_KEY, RAW_VALUE));
                        recordsReturned += numMessages;
                        return new ConsumerRecords<>(
                                numMessages > 0 ?
                                        Collections.singletonMap(new TopicPartition(TOPIC, PARTITION), records) :
                                        Collections.<TopicPartition, List<ConsumerRecord<byte[], byte[]>>>emptyMap()
                        );
                    }
                });
    }

    private void expectConvertMessages(final int numMessages) {
        EasyMock.expect(keyConverter.toConnectData(TOPIC, RAW_KEY)).andReturn(new SchemaAndValue(KEY_SCHEMA, KEY)).times(numMessages);
        EasyMock.expect(valueConverter.toConnectData(TOPIC, RAW_VALUE)).andReturn(new SchemaAndValue(VALUE_SCHEMA, VALUE)).times(numMessages);
    }

    private abstract static class TestSinkTask extends SinkTask  {
    }

}
