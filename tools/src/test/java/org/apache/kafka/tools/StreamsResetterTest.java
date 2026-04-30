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
package org.apache.kafka.tools;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.MemberToRemove;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.protocol.Errors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Constructor;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(value = 600)
public class StreamsResetterTest {
    private static final String TOPIC = "topic1";
    private final StreamsResetter streamsResetter = new StreamsResetter();
    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name());
    private final TopicPartition topicPartition = new TopicPartition(TOPIC, 0);
    private final Set<TopicPartition> inputTopicPartitions = Set.of(topicPartition);

    @BeforeEach
    public void beforeEach() {
        consumer.assign(List.of(topicPartition));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, new byte[] {}, new byte[] {}));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1L, new byte[] {}, new byte[] {}));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 2L, new byte[] {}, new byte[] {}));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 3L, new byte[] {}, new byte[] {}));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 4L, new byte[] {}, new byte[] {}));
    }

    @Test
    public void testResetToSpecificOffsetWhenBetweenBeginningAndEndOffset() {
        final Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(topicPartition, 4L);
        consumer.updateEndOffsets(endOffsets);

        final Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(topicPartition, 0L);
        consumer.updateBeginningOffsets(beginningOffsets);

        streamsResetter.resetOffsetsTo(consumer, inputTopicPartitions, 2L);

        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        assertEquals(3, records.count());
    }

    @Test
    public void testResetOffsetToSpecificOffsetWhenAfterEndOffset() {
        final long beginningOffset = 5L;
        final long endOffset = 10L;
        final MockConsumer<byte[], byte[]> emptyConsumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name());
        emptyConsumer.assign(List.of(topicPartition));

        final Map<TopicPartition, Long> beginningOffsetsMap = new HashMap<>();
        beginningOffsetsMap.put(topicPartition, beginningOffset);
        emptyConsumer.updateBeginningOffsets(beginningOffsetsMap);

        final Map<TopicPartition, Long> endOffsetsMap = new HashMap<>();
        endOffsetsMap.put(topicPartition, endOffset);
        emptyConsumer.updateEndOffsets(endOffsetsMap);
        // resetOffsetsTo only seeks the offset, but does not commit.
        streamsResetter.resetOffsetsTo(emptyConsumer, inputTopicPartitions, endOffset + 2L);

        final long position = emptyConsumer.position(topicPartition);

        assertEquals(endOffset, position);
    }

    @Test
    public void testResetToSpecificOffsetWhenBeforeBeginningOffset() {
        final Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(topicPartition, 4L);
        consumer.updateEndOffsets(endOffsets);

        final Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(topicPartition, 3L);
        consumer.updateBeginningOffsets(beginningOffsets);

        streamsResetter.resetOffsetsTo(consumer, inputTopicPartitions, 2L);

        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        assertEquals(2, records.count());
    }

    @Test
    public void testResetToSpecificOffsetWhenAfterEndOffset() {
        final Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(topicPartition, 3L);
        consumer.updateEndOffsets(endOffsets);

        final Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(topicPartition, 0L);
        consumer.updateBeginningOffsets(beginningOffsets);

        streamsResetter.resetOffsetsTo(consumer, inputTopicPartitions, 4L);

        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        assertEquals(2, records.count());
    }

    @Test
    public void testShiftOffsetByWhenBetweenBeginningAndEndOffset() {
        final Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(topicPartition, 4L);
        consumer.updateEndOffsets(endOffsets);

        final Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(topicPartition, 0L);
        consumer.updateBeginningOffsets(beginningOffsets);

        streamsResetter.shiftOffsetsBy(consumer, inputTopicPartitions, 3L);

        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        assertEquals(2, records.count());
    }

    @Test
    public void testShiftOffsetByWhenBeforeBeginningOffset() {
        final Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(topicPartition, 4L);
        consumer.updateEndOffsets(endOffsets);

        final Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(topicPartition, 0L);
        consumer.updateBeginningOffsets(beginningOffsets);

        streamsResetter.shiftOffsetsBy(consumer, inputTopicPartitions, -3L);

        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        assertEquals(5, records.count());
    }

    @Test
    public void testShiftOffsetByWhenAfterEndOffset() {
        final Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(topicPartition, 3L);
        consumer.updateEndOffsets(endOffsets);

        final Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(topicPartition, 0L);
        consumer.updateBeginningOffsets(beginningOffsets);

        streamsResetter.shiftOffsetsBy(consumer, inputTopicPartitions, 5L);

        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        assertEquals(2, records.count());
    }

    @Test
    public void testResetUsingPlanWhenBetweenBeginningAndEndOffset() {
        final Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(topicPartition, 4L);
        consumer.updateEndOffsets(endOffsets);

        final Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(topicPartition, 0L);
        consumer.updateBeginningOffsets(beginningOffsets);

        final Map<TopicPartition, Long> topicPartitionsAndOffset = new HashMap<>();
        topicPartitionsAndOffset.put(topicPartition, 3L);
        streamsResetter.resetOffsetsFromResetPlan(consumer, inputTopicPartitions, topicPartitionsAndOffset);

        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        assertEquals(2, records.count());
    }

    @Test
    public void testResetUsingPlanWhenBeforeBeginningOffset() {
        final Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(topicPartition, 4L);
        consumer.updateEndOffsets(endOffsets);

        final Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(topicPartition, 3L);
        consumer.updateBeginningOffsets(beginningOffsets);

        final Map<TopicPartition, Long> topicPartitionsAndOffset = new HashMap<>();
        topicPartitionsAndOffset.put(topicPartition, 1L);
        streamsResetter.resetOffsetsFromResetPlan(consumer, inputTopicPartitions, topicPartitionsAndOffset);

        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        assertEquals(2, records.count());
    }

    @Test
    public void testResetUsingPlanWhenAfterEndOffset() {
        final Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(topicPartition, 3L);
        consumer.updateEndOffsets(endOffsets);

        final Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(topicPartition, 0L);
        consumer.updateBeginningOffsets(beginningOffsets);

        final Map<TopicPartition, Long> topicPartitionsAndOffset = new HashMap<>();
        topicPartitionsAndOffset.put(topicPartition, 5L);
        streamsResetter.resetOffsetsFromResetPlan(consumer, inputTopicPartitions, topicPartitionsAndOffset);

        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        assertEquals(2, records.count());
    }

    @Test
    public void shouldSeekToEndOffset() {
        final Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(topicPartition, 3L);
        consumer.updateEndOffsets(endOffsets);

        final Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(topicPartition, 0L);
        consumer.updateBeginningOffsets(beginningOffsets);

        final Set<TopicPartition> intermediateTopicPartitions = new HashSet<>();
        intermediateTopicPartitions.add(topicPartition);
        streamsResetter.maybeSeekToEnd("g1", consumer, intermediateTopicPartitions);

        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        assertEquals(2, records.count());
    }

    @Test
    public void shouldDeleteTopic() throws InterruptedException, ExecutionException {
        final Cluster cluster = createCluster(1);
        try (final MockAdminClient adminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0))) {
            final TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, cluster.nodeById(0), cluster.nodes(), List.of());
            adminClient.addTopic(false, TOPIC, List.of(topicPartitionInfo), null);
            streamsResetter.doDelete(List.of(TOPIC), adminClient);
            assertEquals(Set.of(), adminClient.listTopics().names().get());
        }
    }

    @Test
    public void shouldDetermineInternalTopicBasedOnTopicName1() {
        assertTrue(StreamsResetter.matchesInternalTopicFormat("appId-named-subscription-response-topic"));
        assertTrue(StreamsResetter.matchesInternalTopicFormat("appId-named-subscription-registration-topic"));
        assertTrue(StreamsResetter.matchesInternalTopicFormat("appId-KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-12323232-topic"));
        assertTrue(StreamsResetter.matchesInternalTopicFormat("appId-KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-12323232-topic"));
    }

    @Test
    public void testResetToDatetimeWhenPartitionIsEmptyResetsToLatestOffset() {
        final long beginningAndEndOffset = 5L; // Empty partition implies beginning offset == end offset
        final MockConsumer<byte[], byte[]> emptyConsumer = new EmptyPartitionConsumer<>(AutoOffsetResetStrategy.EARLIEST.name());
        emptyConsumer.assign(List.of(topicPartition));

        final Map<TopicPartition, Long> beginningOffsetsMap = new HashMap<>();
        beginningOffsetsMap.put(topicPartition, beginningAndEndOffset);
        emptyConsumer.updateBeginningOffsets(beginningOffsetsMap);

        final Map<TopicPartition, Long> endOffsetsMap = new HashMap<>();
        endOffsetsMap.put(topicPartition, beginningAndEndOffset);
        emptyConsumer.updateEndOffsets(endOffsetsMap);

        final long yesterdayTimestamp = Instant.now().minus(Duration.ofDays(1)).toEpochMilli();
        // resetToDatetime only seeks the offset, but does not commit.
        streamsResetter.resetToDatetime(emptyConsumer, inputTopicPartitions, yesterdayTimestamp);

        final long position = emptyConsumer.position(topicPartition);

        assertEquals(beginningAndEndOffset, position);
    }

    @Test
    public void shouldRetryToRemoveMembersOnUnknownMemberIdExceptionAndForce() throws Exception {
        final String groupId = "groupId";

        final Admin adminClient = mock(Admin.class);
        final ConsumerGroupDescription consumerGroupDescription = mock(ConsumerGroupDescription.class);

        when(adminClient.describeConsumerGroups(eq(Set.of(groupId)), any()))
                .thenReturn(new DescribeConsumerGroupsResult(Map.of(groupId, KafkaFutureImpl.completedFuture(consumerGroupDescription))));
        when(adminClient.removeMembersFromConsumerGroup(eq(groupId), any()))
                .thenReturn(createRemoveMembersFromConsumerGroupResult(KafkaFutureImpl.completedFuture(Map.of(new LeaveGroupRequestData.MemberIdentity(), Errors.UNKNOWN_MEMBER_ID)), Set.of()))
                .thenReturn(createRemoveMembersFromConsumerGroupResult(KafkaFutureImpl.completedFuture(Map.of()), Set.of()));
        when(consumerGroupDescription.members()).thenReturn(List.of(mock(MemberDescription.class)));

        streamsResetter.maybeDeleteActiveConsumers(groupId, adminClient, true);

        verify(adminClient, times(2)).removeMembersFromConsumerGroup(eq(groupId), any());
    }

    @Test
    public void shouldFailAfterTooManyRetries() throws Exception {
        final String groupId = "groupId";

        final Admin adminClient = mock(Admin.class);
        final ConsumerGroupDescription consumerGroupDescription = mock(ConsumerGroupDescription.class);

        when(adminClient.describeConsumerGroups(eq(Set.of(groupId)), any()))
                .thenReturn(new DescribeConsumerGroupsResult(Map.of(groupId, KafkaFutureImpl.completedFuture(consumerGroupDescription))));
        when(adminClient.removeMembersFromConsumerGroup(eq(groupId), any()))
                .thenReturn(createRemoveMembersFromConsumerGroupResult(KafkaFutureImpl.completedFuture(Map.of(new LeaveGroupRequestData.MemberIdentity(), Errors.UNKNOWN_MEMBER_ID)), Set.of()));
        when(consumerGroupDescription.members()).thenReturn(List.of(mock(MemberDescription.class)));

        assertThrows(ExecutionException.class, () -> streamsResetter.maybeDeleteActiveConsumers(groupId, adminClient, true));

        verify(adminClient, times(4)).removeMembersFromConsumerGroup(eq(groupId), any());
    }

    @Test
    public void shouldFailIfThereAreMembersAndNotForce() throws Exception {
        final String groupId = "groupId";

        final Admin adminClient = mock(Admin.class);
        final ConsumerGroupDescription consumerGroupDescription = mock(ConsumerGroupDescription.class);

        when(adminClient.describeConsumerGroups(eq(Set.of(groupId)), any()))
                .thenReturn(new DescribeConsumerGroupsResult(Map.of(groupId, KafkaFutureImpl.completedFuture(consumerGroupDescription))));
        when(consumerGroupDescription.members()).thenReturn(List.of(mock(MemberDescription.class)));

        assertThrows(IllegalStateException.class, () -> streamsResetter.maybeDeleteActiveConsumers(groupId, adminClient, false));

        verify(adminClient, never()).removeMembersFromConsumerGroup(eq(groupId), any());
    }

    @Test
    public void shouldRemoveIfThereAreMembersAndForce() throws Exception {
        final String groupId = "groupId";

        final Admin adminClient = mock(Admin.class);
        final ConsumerGroupDescription consumerGroupDescription = mock(ConsumerGroupDescription.class);

        when(adminClient.describeConsumerGroups(eq(Set.of(groupId)), any()))
                .thenReturn(new DescribeConsumerGroupsResult(Map.of(groupId, KafkaFutureImpl.completedFuture(consumerGroupDescription))));
        when(adminClient.removeMembersFromConsumerGroup(eq(groupId), any()))
                .thenReturn(createRemoveMembersFromConsumerGroupResult(KafkaFutureImpl.completedFuture(Map.of()), Set.of()));
        when(consumerGroupDescription.members()).thenReturn(List.of(mock(MemberDescription.class)));

        streamsResetter.maybeDeleteActiveConsumers(groupId, adminClient, true);

        verify(adminClient).removeMembersFromConsumerGroup(eq(groupId), any());
    }

    @Test
    public void shouldIgnoreGroupIdNotFoundException() throws Exception {
        final String groupId = "groupId";

        final Admin adminClient = mock(Admin.class);
        final ConsumerGroupDescription consumerGroupDescription = mock(ConsumerGroupDescription.class);

        final KafkaFutureImpl<ConsumerGroupDescription> future = new KafkaFutureImpl<>();
        future.completeExceptionally(new GroupIdNotFoundException(groupId));
        when(adminClient.describeConsumerGroups(eq(Set.of(groupId)), any()))
                .thenReturn(new DescribeConsumerGroupsResult(Map.of(groupId, future)));
        when(consumerGroupDescription.members()).thenReturn(List.of(mock(MemberDescription.class)));

        streamsResetter.maybeDeleteActiveConsumers(groupId, adminClient, true);

        verify(adminClient, never()).removeMembersFromConsumerGroup(eq(groupId), any());
    }

    private RemoveMembersFromConsumerGroupResult createRemoveMembersFromConsumerGroupResult(final KafkaFuture<Map<LeaveGroupRequestData.MemberIdentity, Errors>> future,
                                                                                            final Set<MemberToRemove> memberInfos) throws Exception {
        final Constructor<RemoveMembersFromConsumerGroupResult> constructor = RemoveMembersFromConsumerGroupResult.class.getDeclaredConstructor(KafkaFuture.class, Set.class);
        constructor.setAccessible(true);
        return constructor.newInstance(future, memberInfos);
    }

    private Cluster createCluster(final int numNodes) {
        final HashMap<Integer, Node> nodes = new HashMap<>();
        for (int i = 0; i < numNodes; ++i) {
            nodes.put(i, new Node(i, "localhost", 8121 + i));
        }
        return new Cluster("mockClusterId", nodes.values(),
            Set.of(), Set.of(),
            Set.of(), nodes.get(0));
    }

    private static class EmptyPartitionConsumer<K, V> extends MockConsumer<K, V> {
        public EmptyPartitionConsumer(final String offsetResetStrategy) {
            super(offsetResetStrategy);
        }

        @Override
        public synchronized Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(final Map<TopicPartition, Long> timestampsToSearch) {
            final Map<TopicPartition, OffsetAndTimestamp> topicPartitionToOffsetAndTimestamp = new HashMap<>();
            timestampsToSearch.keySet().forEach(k -> topicPartitionToOffsetAndTimestamp.put(k, null));
            return topicPartitionToOffsetAndTimestamp;
        }
    }
}
