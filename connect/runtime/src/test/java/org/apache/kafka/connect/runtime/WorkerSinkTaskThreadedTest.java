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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.distributed.ClusterConfigState;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperatorTest;
import org.apache.kafka.connect.runtime.isolation.PluginClassLoader;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.util.ThreadedTest;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@PrepareForTest(WorkerSinkTask.class)
@PowerMockIgnore("javax.management.*")
public class WorkerSinkTaskThreadedTest extends ThreadedTest {

    // These are fixed to keep this code simpler. In this example we assume byte[] raw values
    // with mix of integer/string in Connect
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
    private static final Set<TopicPartition> INITIAL_ASSIGNMENT = new HashSet<>(Arrays.asList(
            TOPIC_PARTITION, TOPIC_PARTITION2, TOPIC_PARTITION3));

    private static final Map<String, String> TASK_PROPS = new HashMap<>();
    private static final long TIMESTAMP = 42L;
    private static final TimestampType TIMESTAMP_TYPE = TimestampType.CREATE_TIME;

    static {
        TASK_PROPS.put(SinkConnector.TOPICS_CONFIG, TOPIC);
        TASK_PROPS.put(TaskConfig.TASK_CLASS_CONFIG, TestSinkTask.class.getName());
    }
    private static final TaskConfig TASK_CONFIG = new TaskConfig(TASK_PROPS);

    private ConnectorTaskId taskId = new ConnectorTaskId("job", 0);
    private TargetState initialState = TargetState.STARTED;
    private Time time;
    private ConnectMetrics metrics;
    @Mock private SinkTask sinkTask;
    private Capture<WorkerSinkTaskContext> sinkTaskContext = EasyMock.newCapture();
    private WorkerConfig workerConfig;
    @Mock
    private PluginClassLoader pluginLoader;
    @Mock private Converter keyConverter;
    @Mock private Converter valueConverter;
    @Mock private HeaderConverter headerConverter;
    @Mock private TransformationChain<SinkRecord> transformationChain;
    private WorkerSinkTask workerTask;
    @Mock private KafkaConsumer<byte[], byte[]> consumer;
    private Capture<ConsumerRebalanceListener> rebalanceListener = EasyMock.newCapture();
    @Mock private TaskStatus.Listener statusListener;
    @Mock private StatusBackingStore statusBackingStore;

    private long recordsReturned;


    @Override
    public void setup() {
        super.setup();
        time = new MockTime();
        metrics = new MockConnectMetrics();
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("offset.storage.file.filename", "/tmp/connect.offsets");
        pluginLoader = PowerMock.createMock(PluginClassLoader.class);
        workerConfig = new StandaloneConfig(workerProps);
        workerTask = new WorkerSinkTask(
                taskId, sinkTask, statusListener, initialState, workerConfig, ClusterConfigState.EMPTY, metrics, keyConverter,
                valueConverter, headerConverter,
                new TransformationChain<>(Collections.emptyList(), RetryWithToleranceOperatorTest.NOOP_OPERATOR),
                consumer, pluginLoader, time, RetryWithToleranceOperatorTest.NOOP_OPERATOR, null, statusBackingStore);

        recordsReturned = 0;
    }

    @After
    public void tearDown() {
        if (metrics != null) metrics.stop();
    }

    @Test
    public void testPollsInBackground() throws Exception {
        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();

        Capture<Collection<SinkRecord>> capturedRecords = expectPolls(1L);
        expectStopTask();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();

        // First iteration initializes partition assignment
        workerTask.iteration();

        // Then we iterate to fetch data
        for (int i = 0; i < 10; i++) {
            workerTask.iteration();
        }
        workerTask.stop();
        workerTask.close();

        // Verify contents match expected values, i.e. that they were translated properly. With max
        // batch size 1 and poll returns 1 message at a time, we should have a matching # of batches
        assertEquals(10, capturedRecords.getValues().size());
        int offset = 0;
        for (Collection<SinkRecord> recs : capturedRecords.getValues()) {
            assertEquals(1, recs.size());
            for (SinkRecord rec : recs) {
                SinkRecord referenceSinkRecord
                        = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, KEY, VALUE_SCHEMA, VALUE, FIRST_OFFSET + offset, TIMESTAMP, TIMESTAMP_TYPE);
                InternalSinkRecord referenceInternalSinkRecord =
                    new InternalSinkRecord(null, referenceSinkRecord);
                assertEquals(referenceInternalSinkRecord, rec);
                offset++;
            }
        }

        PowerMock.verifyAll();
    }

    @Test
    public void testCommit() throws Exception {
        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();
        expectConsumerAssignment(INITIAL_ASSIGNMENT).times(2);

        // Make each poll() take the offset commit interval
        Capture<Collection<SinkRecord>> capturedRecords
                = expectPolls(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT);
        expectOffsetCommit(1L, null, null, 0, true);
        expectStopTask();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();

        // Initialize partition assignment
        workerTask.iteration();
        // Fetch one record
        workerTask.iteration();
        // Trigger the commit
        workerTask.iteration();

        // Commit finishes synchronously for testing so we can check this immediately
        assertEquals(0, workerTask.commitFailures());
        workerTask.stop();
        workerTask.close();

        assertEquals(2, capturedRecords.getValues().size());

        PowerMock.verifyAll();
    }

    @Test
    public void testCommitFailure() throws Exception {
        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();
        expectConsumerAssignment(INITIAL_ASSIGNMENT);

        Capture<Collection<SinkRecord>> capturedRecords = expectPolls(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT);
        expectOffsetCommit(1L, new RuntimeException(), null, 0, true);
        // Should rewind to last known good positions, which in this case will be the offsets loaded during initialization
        // for all topic partitions
        consumer.seek(TOPIC_PARTITION, FIRST_OFFSET);
        PowerMock.expectLastCall();
        consumer.seek(TOPIC_PARTITION2, FIRST_OFFSET);
        PowerMock.expectLastCall();
        consumer.seek(TOPIC_PARTITION3, FIRST_OFFSET);
        PowerMock.expectLastCall();
        expectStopTask();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();

        // Initialize partition assignment
        workerTask.iteration();
        // Fetch some data
        workerTask.iteration();
        // Trigger the commit
        workerTask.iteration();

        assertEquals(1, workerTask.commitFailures());
        assertEquals(false, Whitebox.getInternalState(workerTask, "committing"));
        workerTask.stop();
        workerTask.close();

        PowerMock.verifyAll();
    }

    @Test
    public void testCommitSuccessFollowedByFailure() throws Exception {
        // Validate that we rewind to the correct offsets if a task's preCommit() method throws an exception

        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();
        expectConsumerAssignment(INITIAL_ASSIGNMENT).times(3);
        Capture<Collection<SinkRecord>> capturedRecords = expectPolls(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT);
        expectOffsetCommit(1L, null, null, 0, true);
        expectOffsetCommit(2L, new RuntimeException(), null, 0, true);
        // Should rewind to last known committed positions
        consumer.seek(TOPIC_PARTITION, FIRST_OFFSET + 1);
        PowerMock.expectLastCall();
        consumer.seek(TOPIC_PARTITION2, FIRST_OFFSET);
        PowerMock.expectLastCall();
        consumer.seek(TOPIC_PARTITION3, FIRST_OFFSET);
        PowerMock.expectLastCall();
        expectStopTask();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();

        // Initialize partition assignment
        workerTask.iteration();
        // Fetch some data
        workerTask.iteration();
        // Trigger first commit,
        workerTask.iteration();
        // Trigger second (failing) commit
        workerTask.iteration();

        assertEquals(1, workerTask.commitFailures());
        assertEquals(false, Whitebox.getInternalState(workerTask, "committing"));
        workerTask.stop();
        workerTask.close();

        PowerMock.verifyAll();
    }

    @Test
    public void testCommitConsumerFailure() throws Exception {
        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();
        expectConsumerAssignment(INITIAL_ASSIGNMENT).times(2);

        Capture<Collection<SinkRecord>> capturedRecords
                = expectPolls(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT);
        expectOffsetCommit(1L, null, new Exception(), 0, true);
        expectStopTask();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();

        // Initialize partition assignment
        workerTask.iteration();
        // Fetch some data
        workerTask.iteration();
        // Trigger commit
        workerTask.iteration();

        // TODO Response to consistent failures?
        assertEquals(1, workerTask.commitFailures());
        assertEquals(false, Whitebox.getInternalState(workerTask, "committing"));
        workerTask.stop();
        workerTask.close();

        PowerMock.verifyAll();
    }

    @Test
    public void testCommitTimeout() throws Exception {
        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();
        expectConsumerAssignment(INITIAL_ASSIGNMENT).times(2);

        // Cut down amount of time to pass in each poll so we trigger exactly 1 offset commit
        Capture<Collection<SinkRecord>> capturedRecords
                = expectPolls(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT / 2);
        expectOffsetCommit(2L, null, null, WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_DEFAULT, false);
        expectStopTask();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();

        // Initialize partition assignment
        workerTask.iteration();
        // Fetch some data
        workerTask.iteration();
        workerTask.iteration();
        // Trigger the commit
        workerTask.iteration();
        // Trigger the timeout without another commit
        workerTask.iteration();

        // TODO Response to consistent failures?
        assertEquals(1, workerTask.commitFailures());
        assertEquals(false, Whitebox.getInternalState(workerTask, "committing"));
        workerTask.stop();
        workerTask.close();

        PowerMock.verifyAll();
    }

    @Test
    public void testAssignmentPauseResume() throws Exception {
        // Just validate that the calls are passed through to the consumer, and that where appropriate errors are
        // converted
        expectInitializeTask();
        expectTaskGetTopic(true);

        expectPollInitialAssignment();
        expectOnePoll().andAnswer(() -> {
            assertEquals(new HashSet<>(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2, TOPIC_PARTITION3)),
                    sinkTaskContext.getValue().assignment());
            return null;
        });
        EasyMock.expect(consumer.assignment()).andReturn(new HashSet<>(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2, TOPIC_PARTITION3)));

        expectOnePoll().andAnswer(() -> {
            try {
                sinkTaskContext.getValue().pause(UNASSIGNED_TOPIC_PARTITION);
                fail("Trying to pause unassigned partition should have thrown an Connect exception");
            } catch (ConnectException e) {
                // expected
            }
            sinkTaskContext.getValue().pause(TOPIC_PARTITION, TOPIC_PARTITION2);
            return null;
        });
        consumer.pause(Arrays.asList(UNASSIGNED_TOPIC_PARTITION));
        PowerMock.expectLastCall().andThrow(new IllegalStateException("unassigned topic partition"));
        consumer.pause(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2));
        PowerMock.expectLastCall();

        expectOnePoll().andAnswer(() -> {
            try {
                sinkTaskContext.getValue().resume(UNASSIGNED_TOPIC_PARTITION);
                fail("Trying to resume unassigned partition should have thrown an Connect exception");
            } catch (ConnectException e) {
                // expected
            }

            sinkTaskContext.getValue().resume(TOPIC_PARTITION, TOPIC_PARTITION2);
            return null;
        });
        consumer.resume(Arrays.asList(UNASSIGNED_TOPIC_PARTITION));
        PowerMock.expectLastCall().andThrow(new IllegalStateException("unassigned topic partition"));
        consumer.resume(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2));
        PowerMock.expectLastCall();

        expectStopTask();

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration();
        workerTask.iteration();
        workerTask.iteration();
        workerTask.iteration();
        workerTask.stop();
        workerTask.close();

        PowerMock.verifyAll();
    }

    @Test
    public void testRewind() throws Exception {
        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();

        final long startOffset = 40L;
        final Map<TopicPartition, Long> offsets = new HashMap<>();

        expectOnePoll().andAnswer(() -> {
            offsets.put(TOPIC_PARTITION, startOffset);
            sinkTaskContext.getValue().offset(offsets);
            return null;
        });

        consumer.seek(TOPIC_PARTITION, startOffset);
        EasyMock.expectLastCall();

        expectOnePoll().andAnswer(() -> {
            Map<TopicPartition, Long> offsets1 = sinkTaskContext.getValue().offsets();
            assertEquals(0, offsets1.size());
            return null;
        });

        expectStopTask();
        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration();
        workerTask.iteration();
        workerTask.iteration();
        workerTask.stop();
        workerTask.close();

        PowerMock.verifyAll();
    }

    @Test
    public void testRewindOnRebalanceDuringPoll() throws Exception {
        expectInitializeTask();
        expectTaskGetTopic(true);
        expectPollInitialAssignment();
        expectConsumerAssignment(INITIAL_ASSIGNMENT).times(2);

        expectRebalanceDuringPoll().andAnswer(() -> {
            Map<TopicPartition, Long> offsets = sinkTaskContext.getValue().offsets();
            assertEquals(0, offsets.size());
            return null;
        });

        expectStopTask();
        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration();
        workerTask.iteration();
        workerTask.stop();
        workerTask.close();

        PowerMock.verifyAll();
    }

    private void expectInitializeTask() {

        consumer.subscribe(EasyMock.eq(Arrays.asList(TOPIC)), EasyMock.capture(rebalanceListener));
        PowerMock.expectLastCall();

        sinkTask.initialize(EasyMock.capture(sinkTaskContext));
        PowerMock.expectLastCall();
        sinkTask.start(TASK_PROPS);
        PowerMock.expectLastCall();
    }

    private void expectPollInitialAssignment() {
        expectConsumerAssignment(INITIAL_ASSIGNMENT).times(2);

        sinkTask.open(INITIAL_ASSIGNMENT);
        EasyMock.expectLastCall();

        EasyMock.expect(consumer.poll(Duration.ofMillis(EasyMock.anyLong()))).andAnswer(() -> {
            rebalanceListener.getValue().onPartitionsAssigned(INITIAL_ASSIGNMENT);
            return ConsumerRecords.empty();
        });
        EasyMock.expect(consumer.position(TOPIC_PARTITION)).andReturn(FIRST_OFFSET);
        EasyMock.expect(consumer.position(TOPIC_PARTITION2)).andReturn(FIRST_OFFSET);
        EasyMock.expect(consumer.position(TOPIC_PARTITION3)).andReturn(FIRST_OFFSET);

        sinkTask.put(Collections.emptyList());
        EasyMock.expectLastCall();
    }

    private IExpectationSetters<Set<TopicPartition>> expectConsumerAssignment(Set<TopicPartition> assignment) {
        return EasyMock.expect(consumer.assignment()).andReturn(assignment);
    }

    private void expectStopTask() {
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
    private Capture<Collection<SinkRecord>> expectPolls(final long pollDelayMs) {
        // Stub out all the consumer stream/iterator responses, which we just want to verify occur,
        // but don't care about the exact details here.
        EasyMock.expect(consumer.poll(Duration.ofMillis(EasyMock.anyLong()))).andStubAnswer(
            () -> {
                // "Sleep" so time will progress
                time.sleep(pollDelayMs);
                ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(
                        Collections.singletonMap(
                                new TopicPartition(TOPIC, PARTITION),
                                Arrays.asList(new ConsumerRecord<>(TOPIC, PARTITION, FIRST_OFFSET + recordsReturned, TIMESTAMP, TIMESTAMP_TYPE,
                                    0, 0, RAW_KEY, RAW_VALUE, new RecordHeaders(), Optional.empty()))));
                recordsReturned++;
                return records;
            });
        EasyMock.expect(keyConverter.toConnectData(TOPIC, emptyHeaders(), RAW_KEY)).andReturn(new SchemaAndValue(KEY_SCHEMA, KEY)).anyTimes();
        EasyMock.expect(valueConverter.toConnectData(TOPIC, emptyHeaders(), RAW_VALUE)).andReturn(new SchemaAndValue(VALUE_SCHEMA, VALUE)).anyTimes();

        final Capture<SinkRecord> recordCapture = EasyMock.newCapture();
        EasyMock.expect(transformationChain.apply(EasyMock.capture(recordCapture))).andAnswer(
            recordCapture::getValue).anyTimes();

        Capture<Collection<SinkRecord>> capturedRecords = EasyMock.newCapture(CaptureType.ALL);
        sinkTask.put(EasyMock.capture(capturedRecords));
        EasyMock.expectLastCall().anyTimes();
        return capturedRecords;
    }

    @SuppressWarnings("unchecked")
    private IExpectationSetters<Object> expectOnePoll() {
        // Currently the SinkTask's put() method will not be invoked unless we provide some data, so instead of
        // returning empty data, we return one record. The expectation is that the data will be ignored by the
        // response behavior specified using the return value of this method.
        EasyMock.expect(consumer.poll(Duration.ofMillis(EasyMock.anyLong()))).andAnswer(
            () -> {
                // "Sleep" so time will progress
                time.sleep(1L);
                ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(
                        Collections.singletonMap(
                                new TopicPartition(TOPIC, PARTITION),
                                Arrays.asList(new ConsumerRecord<>(TOPIC, PARTITION, FIRST_OFFSET + recordsReturned, TIMESTAMP, TIMESTAMP_TYPE,
                                    0, 0, RAW_KEY, RAW_VALUE, new RecordHeaders(), Optional.empty()))));
                recordsReturned++;
                return records;
            });
        EasyMock.expect(keyConverter.toConnectData(TOPIC, emptyHeaders(), RAW_KEY)).andReturn(new SchemaAndValue(KEY_SCHEMA, KEY));
        EasyMock.expect(valueConverter.toConnectData(TOPIC, emptyHeaders(), RAW_VALUE)).andReturn(new SchemaAndValue(VALUE_SCHEMA, VALUE));
        sinkTask.put(EasyMock.anyObject(Collection.class));
        return EasyMock.expectLastCall();
    }

    @SuppressWarnings("unchecked")
    private IExpectationSetters<Object> expectRebalanceDuringPoll() {
        final List<TopicPartition> partitions = Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2, TOPIC_PARTITION3);

        final long startOffset = 40L;
        final Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(TOPIC_PARTITION, startOffset);

        EasyMock.expect(consumer.poll(Duration.ofMillis(EasyMock.anyLong()))).andAnswer(
            () -> {
                // "Sleep" so time will progress
                time.sleep(1L);

                sinkTaskContext.getValue().offset(offsets);
                rebalanceListener.getValue().onPartitionsAssigned(partitions);

                ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(
                        Collections.singletonMap(
                                new TopicPartition(TOPIC, PARTITION),
                                Arrays.asList(new ConsumerRecord<>(TOPIC, PARTITION, FIRST_OFFSET + recordsReturned, TIMESTAMP, TIMESTAMP_TYPE,
                                    0, 0, RAW_KEY, RAW_VALUE, new RecordHeaders(), Optional.empty())
                                )));
                recordsReturned++;
                return records;
            });

        EasyMock.expect(consumer.position(TOPIC_PARTITION)).andReturn(FIRST_OFFSET);
        EasyMock.expect(consumer.position(TOPIC_PARTITION2)).andReturn(FIRST_OFFSET);
        EasyMock.expect(consumer.position(TOPIC_PARTITION3)).andReturn(FIRST_OFFSET);

        sinkTask.open(partitions);
        EasyMock.expectLastCall();

        consumer.seek(TOPIC_PARTITION, startOffset);
        EasyMock.expectLastCall();

        EasyMock.expect(keyConverter.toConnectData(TOPIC, emptyHeaders(), RAW_KEY)).andReturn(new SchemaAndValue(KEY_SCHEMA, KEY));
        EasyMock.expect(valueConverter.toConnectData(TOPIC, emptyHeaders(), RAW_VALUE)).andReturn(new SchemaAndValue(VALUE_SCHEMA, VALUE));
        sinkTask.put(EasyMock.anyObject(Collection.class));
        return EasyMock.expectLastCall();
    }

    private Capture<OffsetCommitCallback> expectOffsetCommit(final long expectedMessages,
                                                             final RuntimeException error,
                                                             final Exception consumerCommitError,
                                                             final long consumerCommitDelayMs,
                                                             final boolean invokeCallback) {
        final long finalOffset = FIRST_OFFSET + expectedMessages;

        // All assigned partitions will have offsets committed, but we've only processed messages/updated offsets for one
        final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        offsetsToCommit.put(TOPIC_PARTITION, new OffsetAndMetadata(finalOffset));
        offsetsToCommit.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));
        offsetsToCommit.put(TOPIC_PARTITION3, new OffsetAndMetadata(FIRST_OFFSET));
        sinkTask.preCommit(offsetsToCommit);
        IExpectationSetters<Object> expectation = PowerMock.expectLastCall();
        if (error != null) {
            expectation.andThrow(error).once();
            return null;
        } else {
            expectation.andReturn(offsetsToCommit);
        }

        final Capture<OffsetCommitCallback> capturedCallback = EasyMock.newCapture();
        consumer.commitAsync(EasyMock.eq(offsetsToCommit),
                EasyMock.capture(capturedCallback));
        PowerMock.expectLastCall().andAnswer(() -> {
            time.sleep(consumerCommitDelayMs);
            if (invokeCallback)
                capturedCallback.getValue().onComplete(offsetsToCommit, consumerCommitError);
            return null;
        });
        return capturedCallback;
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

    private RecordHeaders emptyHeaders() {
        return new RecordHeaders();
    }

    private static abstract class TestSinkTask extends SinkTask {
    }
}
