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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.ProcessingContext;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperatorTest;
import org.apache.kafka.connect.runtime.isolation.PluginClassLoader;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.AdditionalAnswers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;

import java.io.IOException;
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
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class WorkerSinkTaskThreadedTest {

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

    private final ConnectorTaskId taskId = new ConnectorTaskId("job", 0);
    private final TargetState initialState = TargetState.STARTED;
    private Time time;
    private ConnectMetrics metrics;
    @Mock
    private SinkTask sinkTask;
    private final ArgumentCaptor<WorkerSinkTaskContext> sinkTaskContext = ArgumentCaptor.forClass(WorkerSinkTaskContext.class);
    @Mock
    private PluginClassLoader pluginLoader;
    @Mock
    private Converter keyConverter;
    @Mock
    private Converter valueConverter;
    @Mock
    private HeaderConverter headerConverter;
    @Mock
    private TransformationChain<ConsumerRecord<byte[], byte[]>, SinkRecord> transformationChain;
    private WorkerSinkTask workerTask;
    @Mock
    private KafkaConsumer<byte[], byte[]> consumer;
    private final ArgumentCaptor<ConsumerRebalanceListener> rebalanceListener = ArgumentCaptor.forClass(ConsumerRebalanceListener.class);
    @Mock
    private TaskStatus.Listener statusListener;
    @Mock
    private StatusBackingStore statusBackingStore;
    @Mock
    private ErrorHandlingMetrics errorHandlingMetrics;

    private long recordsReturned;

    private final Function<Long, Map<TopicPartition, OffsetAndMetadata>> offsetsToCommitFn = expectedMessages -> {
        final long finalOffset = FIRST_OFFSET + expectedMessages;

        // All assigned partitions will have offsets committed, but we've only processed messages/updated offsets for one
        final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        offsetsToCommit.put(TOPIC_PARTITION, new OffsetAndMetadata(finalOffset));
        offsetsToCommit.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));
        offsetsToCommit.put(TOPIC_PARTITION3, new OffsetAndMetadata(FIRST_OFFSET));

        return offsetsToCommit;
    };

    @BeforeEach
    public void setup() {
        time = new MockTime();
        metrics = new MockConnectMetrics();
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("offset.storage.file.filename", "/tmp/connect.offsets");
        WorkerConfig workerConfig = new StandaloneConfig(workerProps);
        workerTask = new WorkerSinkTask(
                taskId, sinkTask, statusListener, initialState, workerConfig, ClusterConfigState.EMPTY, metrics, keyConverter,
                valueConverter, errorHandlingMetrics, headerConverter, transformationChain,
                consumer, pluginLoader, time, RetryWithToleranceOperatorTest.noopOperator(), null, statusBackingStore,
                Collections::emptyList);
        recordsReturned = 0;
    }

    @AfterEach
    public void tearDown() {
        if (metrics != null) metrics.stop();
    }

    @Test
    public void testPollsInBackground() {
        expectTaskGetTopic();
        expectInitialAssignment();
        expectPolls(1L);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        // First iteration initializes partition assignment
        workerTask.iteration();
        verifyInitialAssignment();

        // Then we iterate to fetch data
        for (int i = 0; i < 10; i++) {
            workerTask.iteration();
        }
        verifyTaskGetTopic(10);

        workerTask.stop();
        workerTask.close();
        verifyStopTask();

        ArgumentCaptor<Collection<SinkRecord>> capturedRecords = ArgumentCaptor.forClass(Collection.class);
        verify(sinkTask, times(11)).put(capturedRecords.capture());

        // Verify contents match expected values, i.e. that they were translated properly. With max
        // batch size 1 and poll returns 1 message at a time, we should have a matching # of batches + initial assignment
        assertEquals(11, capturedRecords.getAllValues().size());
        // First poll() returned no records as it just triggered a rebalance
        assertTrue(capturedRecords.getAllValues().get(0).isEmpty());

        int offset = 0;
        List<Collection<SinkRecord>> filteredRecords =
                capturedRecords.getAllValues().subList(1, capturedRecords.getAllValues().size() - 1);

        for (Collection<SinkRecord> recs : filteredRecords) {
            assertEquals(1, recs.size());
            for (SinkRecord rec : recs) {
                SinkRecord referenceSinkRecord
                        = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, KEY, VALUE_SCHEMA, VALUE, FIRST_OFFSET + offset, TIMESTAMP, TIMESTAMP_TYPE);
                ConsumerRecord<byte[], byte[]> referenceConsumerRecord
                        = new ConsumerRecord<>(TOPIC, PARTITION, FIRST_OFFSET + offset, null, null);
                ProcessingContext<ConsumerRecord<byte[], byte[]>> context = new ProcessingContext<>(referenceConsumerRecord);
                InternalSinkRecord referenceInternalSinkRecord = new InternalSinkRecord(context, referenceSinkRecord);
                assertEquals(referenceInternalSinkRecord, rec);
                offset++;
            }
        }
    }

    @Test
    public void testCommit() {
        long expectedMessages = 1L;
        expectTaskGetTopic();
        expectInitialAssignment();
        expectPolls(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT);
        ExpectOffsetCommitCommand command = new ExpectOffsetCommitCommand(
                expectedMessages, null, null, 0, true);
        expectPreCommit(command);
        expectOffsetCommit(command);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        // Initialize partition assignment
        workerTask.iteration();
        verifyInitialAssignment();

        // Fetch one record
        workerTask.iteration();
        // Trigger the commit
        workerTask.iteration();

        // Commit finishes synchronously for testing so we can check this immediately
        assertEquals(0, workerTask.commitFailures());
        workerTask.stop();
        workerTask.close();

        verifyTaskGetTopic(2);
        verifyStopTask();
        ArgumentCaptor<Collection<SinkRecord>> capturedRecords = ArgumentCaptor.forClass(Collection.class);
        verify(sinkTask, times(3)).put(capturedRecords.capture());

        assertEquals(3, capturedRecords.getAllValues().size());
        verifyOffsetCommit(expectedMessages);
    }

    @Test
    public void testCommitFailure() {
        expectTaskGetTopic();
        expectInitialAssignment();
        expectPolls(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT);
        expectPreCommit(new ExpectOffsetCommitCommand(
                1L, new RuntimeException(), null, 0, true));

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        // Initialize partition assignment
        workerTask.iteration();
        verifyInitialAssignment();

        // Fetch some data
        workerTask.iteration();
        // Trigger the commit
        workerTask.iteration();

        assertEquals(1, workerTask.commitFailures());
        assertFalse(workerTask.isCommitting());

        workerTask.stop();
        workerTask.close();
        verifyStopTask();
        verifyTaskGetTopic(2);

        // Should rewind to last known good positions, which in this case will be the offsets loaded during initialization
        // for all topic partitions
        verify(consumer).seek(TOPIC_PARTITION, FIRST_OFFSET);
        verify(consumer).seek(TOPIC_PARTITION2, FIRST_OFFSET);
        verify(consumer).seek(TOPIC_PARTITION3, FIRST_OFFSET);
    }

    @Test
    public void testCommitSuccessFollowedByFailure() {
        // Validate that we rewind to the correct offsets if a task's preCommit() method throws an exception
        expectTaskGetTopic();
        expectInitialAssignment();
        expectPolls(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT);
        ExpectOffsetCommitCommand[] commands = new ExpectOffsetCommitCommand[]{
            new ExpectOffsetCommitCommand(1L, null, null, 0, true),
            new ExpectOffsetCommitCommand(2L, new RuntimeException(), null, 0, true)
        };
        expectPreCommit(commands);
        expectOffsetCommit(commands);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        // Initialize partition assignment
        workerTask.iteration();
        verifyInitialAssignment();

        // Fetch some data
        workerTask.iteration();
        // Trigger first commit,
        workerTask.iteration();
        // Trigger second (failing) commit
        workerTask.iteration();

        // Should rewind to last known committed positions
        verify(consumer).seek(TOPIC_PARTITION, FIRST_OFFSET + 1);
        verify(consumer).seek(TOPIC_PARTITION2, FIRST_OFFSET);
        verify(consumer).seek(TOPIC_PARTITION3, FIRST_OFFSET);

        assertEquals(1, workerTask.commitFailures());
        assertFalse(workerTask.isCommitting());
        workerTask.stop();
        workerTask.close();
        verifyStopTask();
        verifyTaskGetTopic(3);
    }

    @Test
    public void testCommitConsumerFailure() {
        expectTaskGetTopic();
        expectInitialAssignment();
        expectPolls(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT);
        ExpectOffsetCommitCommand command = new ExpectOffsetCommitCommand(
                1L, null, new Exception(), 0, true);
        expectPreCommit(command);
        expectOffsetCommit(command);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        // Initialize partition assignment
        workerTask.iteration();
        verifyInitialAssignment();

        // Fetch some data
        workerTask.iteration();
        // Trigger commit
        workerTask.iteration();

        // TODO Response to consistent failures?
        assertEquals(1, workerTask.commitFailures());
        assertFalse(workerTask.isCommitting());

        workerTask.stop();
        workerTask.close();
        verifyStopTask();
        verifyTaskGetTopic(2);
    }

    @Test
    public void testCommitTimeout() {
        expectTaskGetTopic();
        expectInitialAssignment();
        // Cut down amount of time to pass in each poll so we trigger exactly 1 offset commit
        expectPolls(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT / 2);
        ExpectOffsetCommitCommand command = new ExpectOffsetCommitCommand(
                2L, null, null, WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_DEFAULT, false);
        expectPreCommit(command);
        expectOffsetCommit(command);

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        // Initialize partition assignment
        workerTask.iteration();
        verifyInitialAssignment();

        // Fetch some data
        workerTask.iteration();
        workerTask.iteration();
        // Trigger the commit
        workerTask.iteration();
        // Trigger the timeout without another commit
        workerTask.iteration();

        // TODO Response to consistent failures?
        assertEquals(1, workerTask.commitFailures());
        assertFalse(workerTask.isCommitting());
        workerTask.stop();
        workerTask.close();
        verifyStopTask();
        verifyTaskGetTopic(4);
    }

    @Test
    public void testAssignmentPauseResume() {
        // Just validate that the calls are passed through to the consumer, and that where appropriate errors are
        // converted
        expectTaskGetTopic();
        expectInitialAssignment();
        expectPolls(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT);

        doAnswer(invocation -> {
            return null; // initial assignment
        }).doAnswer(invocation -> {
            assertEquals(new HashSet<>(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2, TOPIC_PARTITION3)), sinkTaskContext.getValue().assignment());
            return null;
        }).doAnswer(invocation -> {
            try {
                sinkTaskContext.getValue().pause(UNASSIGNED_TOPIC_PARTITION);
                fail("Trying to pause unassigned partition should have thrown an Connect exception");
            } catch (ConnectException e) {
                // expected
            }
            sinkTaskContext.getValue().pause(TOPIC_PARTITION, TOPIC_PARTITION2);
            return null;
        }).doAnswer(invocation -> {
            try {
                sinkTaskContext.getValue().resume(UNASSIGNED_TOPIC_PARTITION);
                fail("Trying to resume unassigned partition should have thrown an Connect exception");
            } catch (ConnectException e) {
                // expected
            }
            sinkTaskContext.getValue().resume(TOPIC_PARTITION, TOPIC_PARTITION2);
            return null;
        }).when(sinkTask).put(any(Collection.class));

        doThrow(new IllegalStateException("unassigned topic partition")).when(consumer).pause(singletonList(UNASSIGNED_TOPIC_PARTITION));
        doAnswer(invocation -> null).when(consumer).pause(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2));

        doThrow(new IllegalStateException("unassigned topic partition")).when(consumer).resume(singletonList(UNASSIGNED_TOPIC_PARTITION));
        doAnswer(invocation -> null).when(consumer).resume(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2));

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        workerTask.iteration();
        verifyInitialAssignment();

        workerTask.iteration();
        workerTask.iteration();
        workerTask.iteration();
        workerTask.stop();
        workerTask.close();
        verifyStopTask();
        verifyTaskGetTopic(3);

        verify(consumer, atLeastOnce()).pause(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2));
        verify(consumer, atLeastOnce()).resume(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2));
    }

    @Test
    public void testRewind() {
        expectTaskGetTopic();
        expectInitialAssignment();
        expectPolls(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_DEFAULT);

        final long startOffset = 40L;
        final Map<TopicPartition, Long> offsets = new HashMap<>();

        doAnswer(invocation -> {
            return null; // initial assignment
        }).doAnswer(invocation -> {
            offsets.put(TOPIC_PARTITION, startOffset);
            sinkTaskContext.getValue().offset(offsets);
            return null;
        }).doAnswer(invocation -> {
            Map<TopicPartition, Long> offsets1 = sinkTaskContext.getValue().offsets();
            assertEquals(0, offsets1.size());
            return null;
        }).when(sinkTask).put(any(Collection.class));

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        workerTask.iteration();
        verifyInitialAssignment();

        workerTask.iteration();
        workerTask.iteration();
        verify(consumer).seek(TOPIC_PARTITION, startOffset);

        workerTask.stop();
        workerTask.close();
        verifyStopTask();
        verifyTaskGetTopic(2);
    }

    @Test
    public void testRewindOnRebalanceDuringPoll() {
        final long startOffset = 40L;

        expectTaskGetTopic();
        expectInitialAssignment();
        expectRebalanceDuringPoll(startOffset);

        doAnswer(invocation -> {
            return null; // initial assignment
        }).doAnswer(invocation -> {
            Map<TopicPartition, Long> offsets = sinkTaskContext.getValue().offsets();
            assertEquals(0, offsets.size());
            return null;

        }).when(sinkTask).put(any(Collection.class));

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        verifyInitializeTask();

        workerTask.iteration();
        verifyInitialAssignment();

        workerTask.iteration();
        verify(consumer).seek(TOPIC_PARTITION, startOffset);

        workerTask.stop();
        workerTask.close();
        verifyStopTask();
        verifyTaskGetTopic(1);
    }

    private void verifyInitializeTask() {
        verify(consumer).subscribe(eq(singletonList(TOPIC)), rebalanceListener.capture());
        verify(sinkTask).initialize(sinkTaskContext.capture());
        verify(sinkTask).start(TASK_PROPS);
    }


    private void expectInitialAssignment() {
        when(consumer.assignment()).thenReturn(INITIAL_ASSIGNMENT);
        INITIAL_ASSIGNMENT.forEach(tp -> when(consumer.position(tp)).thenReturn(FIRST_OFFSET));
    }

    private void verifyInitialAssignment() {
        verify(sinkTask).open(INITIAL_ASSIGNMENT);
        verify(sinkTask).put(Collections.emptyList());
    }

    private void verifyStopTask() {
        verify(sinkTask).stop();

        // No offset commit since it happens in the mocked worker thread, but the main thread does need to wake up the
        // consumer so it exits quickly
        verify(consumer).wakeup();

        verify(consumer).close();

        try {
            verify(headerConverter).close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Note that this can only be called once per test currently
    private void expectPolls(final long pollDelayMs) {
        // Stub out all the consumer stream/iterator responses, which we just want to verify occur,
        // but don't care about the exact details here.
        when(consumer.poll(any(Duration.class))).thenAnswer(invocation -> {
            rebalanceListener.getValue().onPartitionsAssigned(INITIAL_ASSIGNMENT);
            return ConsumerRecords.empty();
        }).thenAnswer((Answer<ConsumerRecords<byte[], byte[]>>) invocation -> {
            // "Sleep" so time will progress
            time.sleep(pollDelayMs);

            TopicPartition topicPartition = new TopicPartition(TOPIC, PARTITION);
            ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(TOPIC, PARTITION, FIRST_OFFSET + recordsReturned, TIMESTAMP, TIMESTAMP_TYPE, 0, 0, RAW_KEY, RAW_VALUE, emptyHeaders(), Optional.empty());
            ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(Map.of(topicPartition, List.of(consumerRecord)),
                Map.of(topicPartition, new OffsetAndMetadata(FIRST_OFFSET + recordsReturned + 1, Optional.empty(), "")));
            recordsReturned++;
            return records;
        });
        when(keyConverter.toConnectData(TOPIC, emptyHeaders(), RAW_KEY)).thenReturn(new SchemaAndValue(KEY_SCHEMA, KEY));
        when(valueConverter.toConnectData(TOPIC, emptyHeaders(), RAW_VALUE)).thenReturn(new SchemaAndValue(VALUE_SCHEMA, VALUE));
        when(transformationChain.apply(any(), any(SinkRecord.class))).thenAnswer(AdditionalAnswers.returnsSecondArg());
    }

    @SuppressWarnings("SameParameterValue")
    private void expectRebalanceDuringPoll(long startOffset) {
        final List<TopicPartition> partitions = Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2, TOPIC_PARTITION3);

        final Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(TOPIC_PARTITION, startOffset);

        when(consumer.poll(any(Duration.class))).thenAnswer(invocation -> {
            rebalanceListener.getValue().onPartitionsAssigned(INITIAL_ASSIGNMENT);
            return ConsumerRecords.empty();
        }).thenAnswer((Answer<ConsumerRecords<byte[], byte[]>>) invocation -> {
            // "Sleep" so time will progress
            time.sleep(1L);

            sinkTaskContext.getValue().offset(offsets);
            rebalanceListener.getValue().onPartitionsAssigned(partitions);

            TopicPartition topicPartition = new TopicPartition(TOPIC, PARTITION);
            ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(
                    TOPIC, PARTITION, FIRST_OFFSET + recordsReturned, TIMESTAMP, TIMESTAMP_TYPE,
                    0, 0, RAW_KEY, RAW_VALUE, emptyHeaders(), Optional.empty());
            ConsumerRecords<byte[], byte[]> records =
                    new ConsumerRecords<>(Map.of(topicPartition, List.of(consumerRecord)),
                        Map.of(topicPartition, new OffsetAndMetadata(FIRST_OFFSET + recordsReturned + 1, Optional.empty(), "")));
            recordsReturned++;
            return records;
        });

        when(keyConverter.toConnectData(TOPIC, emptyHeaders(), RAW_KEY)).thenReturn(new SchemaAndValue(KEY_SCHEMA, KEY));
        when(valueConverter.toConnectData(TOPIC, emptyHeaders(), RAW_VALUE)).thenReturn(new SchemaAndValue(VALUE_SCHEMA, VALUE));
    }

    private void expectPreCommit(ExpectOffsetCommitCommand... commands) {
        doAnswer(new Answer<>() {
            int index = 0;

            @Override
            public Object answer(InvocationOnMock invocation) {
                ExpectOffsetCommitCommand commitCommand = commands[index++];
                // All assigned partitions will have offsets committed, but we've only processed messages/updated 
                // offsets for one
                final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit =
                        offsetsToCommitFn.apply(commitCommand.expectedMessages);

                if (commitCommand.error != null) {
                    throw commitCommand.error;
                } else {
                    return offsetsToCommit;
                }
            }
        }).when(sinkTask).preCommit(anyMap());
    }
    
    private void expectOffsetCommit(ExpectOffsetCommitCommand... commands) {
        doAnswer(new Answer<>() {
            int index = 0;

            @Override
            public Object answer(InvocationOnMock invocation) {
                ExpectOffsetCommitCommand commitCommand = commands[index++];

                time.sleep(commitCommand.consumerCommitDelayMs);
                if (commitCommand.invokeCallback) {
                    OffsetCommitCallback callback = invocation.getArgument(1);
                    final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = offsetsToCommitFn.apply(commitCommand.expectedMessages);

                    callback.onComplete(offsetsToCommit, commitCommand.consumerCommitError);
                }
                return null;
            }
        }).when(consumer).commitAsync(anyMap(), any(OffsetCommitCallback.class));
    }

    private void verifyOffsetCommit(final long expectedMessages) {
        final long finalOffset = FIRST_OFFSET + expectedMessages;

        // All assigned partitions will have offsets committed, but we've only processed messages/updated offsets for one
        final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        offsetsToCommit.put(TOPIC_PARTITION, new OffsetAndMetadata(finalOffset));
        offsetsToCommit.put(TOPIC_PARTITION2, new OffsetAndMetadata(FIRST_OFFSET));
        offsetsToCommit.put(TOPIC_PARTITION3, new OffsetAndMetadata(FIRST_OFFSET));

        verify(sinkTask).preCommit(offsetsToCommit);
        verify(consumer).commitAsync(eq(offsetsToCommit), any(OffsetCommitCallback.class));
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

    private RecordHeaders emptyHeaders() {
        return new RecordHeaders();
    }

    private abstract static class TestSinkTask extends SinkTask {
    }

    private static class ExpectOffsetCommitCommand {
        final long expectedMessages;
        final RuntimeException error;
        final Exception consumerCommitError;
        final long consumerCommitDelayMs;
        final boolean invokeCallback;

        private ExpectOffsetCommitCommand(long expectedMessages, RuntimeException error, Exception consumerCommitError, long consumerCommitDelayMs, boolean invokeCallback) {
            this.expectedMessages = expectedMessages;
            this.error = error;
            this.consumerCommitError = consumerCommitError;
            this.consumerCommitDelayMs = consumerCommitDelayMs;
            this.invokeCallback = invokeCallback;
        }
    }
}
