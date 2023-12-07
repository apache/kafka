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
package org.apache.kafka.clients.consumer.internals;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.Metadata.LeaderAndEpoch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.AssignmentChangeApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ListOffsetsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.NewTopicsMetadataUpdateRequestEvent;
import org.apache.kafka.clients.consumer.internals.events.FetchCommittedOffsetsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.SubscriptionChangeApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.UnsubscribeApplicationEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

@SuppressWarnings("unchecked")
public class AsyncKafkaConsumerUnitTest {

    private AsyncKafkaConsumer<String, String> consumer = null;

    private final Time time = new MockTime();
    private final FetchCollector<String, String> fetchCollector = mock(FetchCollector.class);
    private final ApplicationEventHandler applicationEventHandler = mock(ApplicationEventHandler.class);
    private final ConsumerMetadata metadata = mock(ConsumerMetadata.class);

    @AfterEach
    public void resetAll() {
        if (consumer != null) {
            consumer.close();
        }
        consumer = null;
        Mockito.framework().clearInlineMocks();
    }

    private AsyncKafkaConsumer<String, String> setup() {
        Properties props = requiredConsumerProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-id");
        final ConsumerConfig config = new ConsumerConfig(props);
        return setup(config);
    }

    private AsyncKafkaConsumer<String, String> setupWithEmptyGroupId() {
        Properties props = requiredConsumerProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "");
        final ConsumerConfig config = new ConsumerConfig(props);
        return setup(config);
    }

    private AsyncKafkaConsumer<String, String> setup(ConsumerConfig config) {
        return new AsyncKafkaConsumer<>(
            config,
            new StringDeserializer(),
            new StringDeserializer(),
            time,
            (a,b,c,d,e,f) -> applicationEventHandler,
            (a,b,c,d,e,f,g) -> fetchCollector,
            (a,b,c,d) -> metadata
        );
    }

    @Test
    public void testGroupIdNull() {
        final Properties props = requiredConsumerProperties();
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 10000);
        props.put(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED, true);
        final ConsumerConfig config = new ConsumerConfig(props);

        consumer = setup(config);
        assertFalse(config.unused().contains(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));
        assertFalse(config.unused().contains(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED));
    }

    @Test
    public void testGroupIdNotNullAndValid() {
        final Properties props = requiredConsumerProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumerGroupA");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 10000);
        props.put(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED, true);
        final ConsumerConfig config = new ConsumerConfig(props);

        consumer = setup(config);
        assertTrue(config.unused().contains(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));
        assertTrue(config.unused().contains(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED));
    }

    @Test
    public void testGroupIdEmpty() {
        testInvalidGroupId("");
    }

    @Test
    public void testGroupIdOnlyWhitespaces() {
        testInvalidGroupId("       ");
    }

    private void testInvalidGroupId(final String groupId) {
        final Properties props = requiredConsumerProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        final ConsumerConfig config = new ConsumerConfig(props);

        final Exception exception = assertThrows(
            KafkaException.class,
            () -> consumer = setup(config)
        );

        assertEquals("Failed to construct kafka consumer", exception.getMessage());
    }

    private Properties requiredConsumerProperties() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        return props;
    }

    @Test
    public void testInvalidGroupId() {
        KafkaException e = assertThrows(KafkaException.class, this::setupWithEmptyGroupId);
        assertTrue(e.getCause() instanceof InvalidGroupIdException);
    }

    @Test
    public void testAssign() {
        consumer = setup();
        final TopicPartition tp = new TopicPartition("foo", 3);
        consumer.assign(singleton(tp));
        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().contains(tp));
        verify(applicationEventHandler).add(any(AssignmentChangeApplicationEvent.class));
        verify(applicationEventHandler).add(any(NewTopicsMetadataUpdateRequestEvent.class));
    }

    @Test
    public void testAssignOnNullTopicPartition() {
        consumer = setup();
        assertThrows(IllegalArgumentException.class, () -> consumer.assign(null));
    }

    @Test
    public void testAssignOnEmptyTopicPartition() {
        consumer = setup();

        Mockito.doAnswer(invocation -> {
            CompletableApplicationEvent<?> event = invocation.getArgument(0);
            assertTrue(event instanceof UnsubscribeApplicationEvent);
            event.future().complete(null);
            return null;
        }).when(applicationEventHandler).add(any());

        consumer.assign(Collections.emptyList());
        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().isEmpty());
    }

    @Test
    public void testAssignOnNullTopicInPartition() {
        consumer = setup();
        assertThrows(IllegalArgumentException.class, () -> consumer.assign(singleton(new TopicPartition(null, 0))));
    }

    @Test
    public void testAssignOnEmptyTopicInPartition() {
        consumer = setup();
        assertThrows(IllegalArgumentException.class, () -> consumer.assign(singleton(new TopicPartition("  ", 0))));
    }

    @Test
    public void testFencedInstanceException() {
        consumer = setup();
        Mockito.doAnswer(invocation -> {
            CompletableApplicationEvent<?> event = invocation.getArgument(0);
            assertTrue(event instanceof CommitApplicationEvent);
            event.future().completeExceptionally(Errors.FENCED_INSTANCE_ID.exception());
            return null;
        }).when(applicationEventHandler).add(any());

        assertDoesNotThrow(() -> consumer.commitAsync());
    }

    @Test
    public void testCommitSyncLeaderEpochUpdate() {
        consumer = setup();
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = new HashMap<>();
        topicPartitionOffsets.put(t0, new OffsetAndMetadata(10L, Optional.of(2), ""));
        topicPartitionOffsets.put(t1, new OffsetAndMetadata(20L, Optional.of(1), ""));
        Mockito.doAnswer(invocation -> {
            CompletableApplicationEvent<?> event = invocation.getArgument(0);
            assertTrue(event instanceof CommitApplicationEvent);
            event.future().complete(null);
            return null;
        }).when(applicationEventHandler).add(any());

        assertDoesNotThrow(() -> consumer.commitSync(topicPartitionOffsets));

        verify(metadata).updateLastSeenEpochIfNewer(t0, 2);
        verify(metadata).updateLastSeenEpochIfNewer(t1, 1);
    }

    @Test
    public void testCommitAsyncWithNullCallback() {
        consumer = setup();
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(t0, new OffsetAndMetadata(10L));
        offsets.put(t1, new OffsetAndMetadata(20L));

        consumer.commitAsync(offsets, null);

        final ArgumentCaptor<CommitApplicationEvent> commitEventCaptor = ArgumentCaptor.forClass(CommitApplicationEvent.class);
        verify(applicationEventHandler).add(commitEventCaptor.capture());
        final CommitApplicationEvent commitEvent = commitEventCaptor.getValue();
        assertEquals(offsets, commitEvent.offsets());
        assertDoesNotThrow(() -> commitEvent.future().complete(null));
        assertDoesNotThrow(() -> consumer.commitAsync(offsets, null));
    }

    @Test
    public void testCommitAsyncLeaderEpochUpdate() {
        consumer = setup();
        OffsetCommitCallback callback = mock(OffsetCommitCallback.class);
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = new HashMap<>();
        topicPartitionOffsets.put(t0, new OffsetAndMetadata(10L, Optional.of(2), ""));
        topicPartitionOffsets.put(t1, new OffsetAndMetadata(20L, Optional.of(1), ""));
        consumer.assign(Arrays.asList(t0, t1));

        consumer.commitAsync(topicPartitionOffsets, callback);

        verify(metadata).updateLastSeenEpochIfNewer(t0, 2);
        verify(metadata).updateLastSeenEpochIfNewer(t1, 1);
    }

    @Test
    public void testCommitted() {
        consumer = setup();
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = mockTopicPartitionOffset();

        when(applicationEventHandler.addAndGet(any(), any())).thenAnswer(invocation -> {
            CompletableApplicationEvent<?> event = invocation.getArgument(0);
            assertTrue(event instanceof FetchCommittedOffsetsApplicationEvent);
            return topicPartitionOffsets;
        });

        assertDoesNotThrow(() -> consumer.committed(topicPartitionOffsets.keySet(), Duration.ofMillis(1000)));
    }

    @Test
    public void testCommittedExceptionThrown() {
        consumer = setup();
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = mockTopicPartitionOffset();
        when(applicationEventHandler.addAndGet(any(), any())).thenAnswer(invocation -> {
            CompletableApplicationEvent<?> event = invocation.getArgument(0);
            assertTrue(event instanceof FetchCommittedOffsetsApplicationEvent);
            throw new KafkaException("Test exception");
        });

        assertThrows(KafkaException.class, () -> consumer.committed(topicPartitionOffsets.keySet(), Duration.ofMillis(1000)));
    }

    @Test
    public void testCommittedLeaderEpochUpdate() {
        consumer = setup();
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        final TopicPartition t2 = new TopicPartition("t0", 4);
        HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = new HashMap<>();
        topicPartitionOffsets.put(t0, new OffsetAndMetadata(10L, Optional.of(2), ""));
        topicPartitionOffsets.put(t1, null);
        topicPartitionOffsets.put(t2, new OffsetAndMetadata(20L, Optional.of(3), ""));
        when(applicationEventHandler.addAndGet(any(), any())).thenAnswer(invocation -> {
            CompletableApplicationEvent<?> event = invocation.getArgument(0);
            assertTrue(event instanceof FetchCommittedOffsetsApplicationEvent);
            return topicPartitionOffsets;
        });

        assertDoesNotThrow(() -> consumer.committed(topicPartitionOffsets.keySet(), Duration.ofMillis(1000)));

        verify(metadata).updateLastSeenEpochIfNewer(t0, 2);
        verify(metadata).updateLastSeenEpochIfNewer(t2, 3);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testPollLongThrowsException() {
        consumer = setup();
        Exception e = assertThrows(UnsupportedOperationException.class, () -> consumer.poll(0L));
        assertEquals("Consumer.poll(long) is not supported when \"group.protocol\" is \"consumer\". " +
            "This method is deprecated and will be removed in the next major release.", e.getMessage());
    }

    @Test
    public void testBeginningOffsetsFailsIfNullPartitions() {
        consumer = setup();
        assertThrows(NullPointerException.class, () -> consumer.beginningOffsets(null,
            Duration.ofMillis(1)));
    }

    @Test
    public void testBeginningOffsets() {
        consumer = setup();
        Map<TopicPartition, OffsetAndTimestamp> expectedOffsetsAndTimestamp =
            mockOffsetAndTimestamp();
        Set<TopicPartition> partitions = expectedOffsetsAndTimestamp.keySet();
        doReturn(expectedOffsetsAndTimestamp).when(applicationEventHandler).addAndGet(any(), any());
        Map<TopicPartition, Long> result =
            assertDoesNotThrow(() -> consumer.beginningOffsets(partitions,
                Duration.ofMillis(1)));
        Map<TopicPartition, Long> expectedOffsets = expectedOffsetsAndTimestamp.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
        assertEquals(expectedOffsets, result);
        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(ListOffsetsApplicationEvent.class),
            ArgumentMatchers.isA(Timer.class));
    }

    @Test
    public void testBeginningOffsetsThrowsKafkaExceptionForUnderlyingExecutionFailure() {
        consumer = setup();
        Set<TopicPartition> partitions = mockTopicPartitionOffset().keySet();
        Throwable eventProcessingFailure = new KafkaException("Unexpected failure " +
            "processing List Offsets event");
        doThrow(eventProcessingFailure).when(applicationEventHandler).addAndGet(any(), any());
        Throwable consumerError = assertThrows(KafkaException.class,
            () -> consumer.beginningOffsets(partitions,
                Duration.ofMillis(1)));
        assertEquals(eventProcessingFailure, consumerError);
        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(ListOffsetsApplicationEvent.class), ArgumentMatchers.isA(Timer.class));
    }

    @Test
    public void testBeginningOffsetsTimeoutOnEventProcessingTimeout() {
        consumer = setup();
        doThrow(new TimeoutException()).when(applicationEventHandler).addAndGet(any(), any());
        assertThrows(TimeoutException.class,
            () -> consumer.beginningOffsets(
                Collections.singletonList(new TopicPartition("t1", 0)),
                Duration.ofMillis(1)));
        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(ListOffsetsApplicationEvent.class),
            ArgumentMatchers.isA(Timer.class));
    }

    @Test
    public void testOffsetsForTimesOnNullPartitions() {
        consumer = setup();
        assertThrows(NullPointerException.class, () -> consumer.offsetsForTimes(null,
            Duration.ofMillis(1)));
    }

    @Test
    public void testOffsetsForTimesFailsOnNegativeTargetTimes() {
        consumer = setup();
        assertThrows(IllegalArgumentException.class,
            () -> consumer.offsetsForTimes(Collections.singletonMap(new TopicPartition(
                    "topic1", 1), ListOffsetsRequest.EARLIEST_TIMESTAMP),
                Duration.ofMillis(1)));

        assertThrows(IllegalArgumentException.class,
            () -> consumer.offsetsForTimes(Collections.singletonMap(new TopicPartition(
                    "topic1", 1), ListOffsetsRequest.LATEST_TIMESTAMP),
                Duration.ofMillis(1)));

        assertThrows(IllegalArgumentException.class,
            () -> consumer.offsetsForTimes(Collections.singletonMap(new TopicPartition(
                    "topic1", 1), ListOffsetsRequest.MAX_TIMESTAMP),
                Duration.ofMillis(1)));
    }

    @Test
    public void testOffsetsForTimes() {
        consumer = setup();
        Map<TopicPartition, OffsetAndTimestamp> expectedResult = mockOffsetAndTimestamp();
        Map<TopicPartition, Long> timestampToSearch = mockTimestampToSearch();

        doReturn(expectedResult).when(applicationEventHandler).addAndGet(any(), any());
        Map<TopicPartition, OffsetAndTimestamp> result =
            assertDoesNotThrow(() -> consumer.offsetsForTimes(timestampToSearch, Duration.ofMillis(1)));
        assertEquals(expectedResult, result);
        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(ListOffsetsApplicationEvent.class),
            ArgumentMatchers.isA(Timer.class));
    }

    // This test ensures same behaviour as the current consumer when offsetsForTimes is called
    // with 0 timeout. It should return map with all requested partitions as keys, with null
    // OffsetAndTimestamp as value.
    @Test
    public void testOffsetsForTimesWithZeroTimeout() {
        consumer = setup();
        TopicPartition tp = new TopicPartition("topic1", 0);
        Map<TopicPartition, OffsetAndTimestamp> expectedResult =
            Collections.singletonMap(tp, null);
        Map<TopicPartition, Long> timestampToSearch = Collections.singletonMap(tp, 5L);

        Map<TopicPartition, OffsetAndTimestamp> result =
            assertDoesNotThrow(() -> consumer.offsetsForTimes(timestampToSearch,
                Duration.ofMillis(0)));
        assertEquals(expectedResult, result);
        verify(applicationEventHandler, never()).addAndGet(ArgumentMatchers.isA(ListOffsetsApplicationEvent.class),
            ArgumentMatchers.isA(Timer.class));
    }

    @Test
    public void testSubscribeGeneratesEvent() {
        consumer = setup();
        String topic = "topic1";
        consumer.subscribe(singletonList(topic));
        assertEquals(singleton(topic), consumer.subscription());
        assertTrue(consumer.assignment().isEmpty());
        verify(applicationEventHandler).add(ArgumentMatchers.isA(SubscriptionChangeApplicationEvent.class));
    }

    @Test
    public void testUnsubscribeGeneratesUnsubscribeEvent() {
        consumer = setup();
        Mockito.doAnswer(invocation -> {
            CompletableApplicationEvent<?> event = invocation.getArgument(0);
            assertTrue(event instanceof UnsubscribeApplicationEvent);
            event.future().complete(null);
            return null;
        }).when(applicationEventHandler).add(any());

        consumer.unsubscribe();

        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().isEmpty());
    }

    @Test
    public void testSubscribeToEmptyListActsAsUnsubscribe() {
        consumer = setup();

        Mockito.doAnswer(invocation -> {
            CompletableApplicationEvent<?> event = invocation.getArgument(0);
            assertTrue(event instanceof UnsubscribeApplicationEvent);
            event.future().complete(null);
            return null;
        }).when(applicationEventHandler).add(any());

        consumer.subscribe(Collections.emptyList());
        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().isEmpty());
    }

    @Test
    public void testSubscribeToNullTopicCollection() {
        consumer = setup();
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe((List<String>) null));
    }

    @Test
    public void testSubscriptionOnNullTopic() {
        consumer = setup();
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(singletonList(null)));
    }

    @Test
    public void testSubscriptionOnEmptyTopic() {
        consumer = setup();
        String emptyTopic = "  ";
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(singletonList(emptyTopic)));
    }


    @Test
    public void testWakeupBeforeCallingPoll() {
        consumer = setup();
        final String topicName = "foo";
        final int partition = 3;
        final TopicPartition tp = new TopicPartition(topicName, partition);
        doReturn(Fetch.empty()).when(fetchCollector).collectFetch(any(FetchBuffer.class));
        Map<TopicPartition, OffsetAndMetadata> offsets = mkMap(mkEntry(tp, new OffsetAndMetadata(1)));
        doReturn(offsets).when(applicationEventHandler).addAndGet(any(FetchCommittedOffsetsApplicationEvent.class), any(Timer.class));
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        consumer.assign(singleton(tp));

        consumer.wakeup();

        assertThrows(WakeupException.class, () -> consumer.poll(Duration.ZERO));
        assertDoesNotThrow(() -> consumer.poll(Duration.ZERO));
    }

    @Test
    public void testWakeupAfterEmptyFetch() {
        consumer = setup();
        final String topicName = "foo";
        final int partition = 3;
        final TopicPartition tp = new TopicPartition(topicName, partition);
        doAnswer(invocation -> {
            consumer.wakeup();
            return Fetch.empty();
        }).when(fetchCollector).collectFetch(any(FetchBuffer.class));
        Map<TopicPartition, OffsetAndMetadata> offsets = mkMap(mkEntry(tp, new OffsetAndMetadata(1)));
        doReturn(offsets).when(applicationEventHandler).addAndGet(any(FetchCommittedOffsetsApplicationEvent.class), any(Timer.class));
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        consumer.assign(singleton(tp));

        assertThrows(WakeupException.class, () -> consumer.poll(Duration.ofMinutes(1)));
        assertDoesNotThrow(() -> consumer.poll(Duration.ZERO));
    }

    @Test
    public void testWakeupAfterNonEmptyFetch() {
        consumer = setup();
        final String topicName = "foo";
        final int partition = 3;
        final TopicPartition tp = new TopicPartition(topicName, partition);
        final List<ConsumerRecord<String, String>> records = asList(
            new ConsumerRecord<>(topicName, partition, 2, "key1", "value1"),
            new ConsumerRecord<>(topicName, partition, 3, "key2", "value2")
        );
        doAnswer(invocation -> {
            consumer.wakeup();
            return Fetch.forPartition(tp, records, true);
        }).when(fetchCollector).collectFetch(Mockito.any(FetchBuffer.class));
        Map<TopicPartition, OffsetAndMetadata> offsets = mkMap(mkEntry(tp, new OffsetAndMetadata(1)));
        doReturn(offsets).when(applicationEventHandler).addAndGet(any(FetchCommittedOffsetsApplicationEvent.class), any(Timer.class));
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        consumer.assign(singleton(tp));

        // since wakeup() is called when the non-empty fetch is returned the wakeup should be ignored
        assertDoesNotThrow(() -> consumer.poll(Duration.ofMinutes(1)));
        // the previously ignored wake-up should not be ignored in the next call
        assertThrows(WakeupException.class, () -> consumer.poll(Duration.ZERO));
    }

    @Test
    public void testClearWakeupTriggerAfterPoll() {
        consumer = setup();
        final String topicName = "foo";
        final int partition = 3;
        final TopicPartition tp = new TopicPartition(topicName, partition);
        final List<ConsumerRecord<String, String>> records = asList(
            new ConsumerRecord<>(topicName, partition, 2, "key1", "value1"),
            new ConsumerRecord<>(topicName, partition, 3, "key2", "value2")
        );
        doReturn(Fetch.forPartition(tp, records, true))
            .when(fetchCollector).collectFetch(any(FetchBuffer.class));
        Map<TopicPartition, OffsetAndMetadata> offsets = mkMap(mkEntry(tp, new OffsetAndMetadata(1)));
        doReturn(offsets).when(applicationEventHandler).addAndGet(any(FetchCommittedOffsetsApplicationEvent.class), any(Timer.class));
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        consumer.assign(singleton(tp));

        consumer.poll(Duration.ZERO);

        assertDoesNotThrow(() -> consumer.poll(Duration.ZERO));
    }

    private HashMap<TopicPartition, OffsetAndMetadata> mockTopicPartitionOffset() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = new HashMap<>();
        topicPartitionOffsets.put(t0, new OffsetAndMetadata(10L));
        topicPartitionOffsets.put(t1, new OffsetAndMetadata(20L));
        return topicPartitionOffsets;
    }

    private HashMap<TopicPartition, OffsetAndTimestamp> mockOffsetAndTimestamp() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, OffsetAndTimestamp> offsetAndTimestamp = new HashMap<>();
        offsetAndTimestamp.put(t0, new OffsetAndTimestamp(5L, 1L));
        offsetAndTimestamp.put(t1, new OffsetAndTimestamp(6L, 3L));
        return offsetAndTimestamp;
    }

    private HashMap<TopicPartition, Long> mockTimestampToSearch() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, Long> timestampToSearch = new HashMap<>();
        timestampToSearch.put(t0, 1L);
        timestampToSearch.put(t1, 2L);
        return timestampToSearch;
    }

}

