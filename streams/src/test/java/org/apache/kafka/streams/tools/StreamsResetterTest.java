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
package org.apache.kafka.streams.tools;

import kafka.tools.StreamsResetter;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamsResetterTest {

    private static final String TOPIC = "topic1";
    private final StreamsResetter streamsResetter = new StreamsResetter();
    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final TopicPartition topicPartition = new TopicPartition(TOPIC, 0);
    private final Set<TopicPartition> inputTopicPartitions = new HashSet<>(Collections.singletonList(topicPartition));

    @Before
    public void setUp() {
        consumer.assign(Collections.singletonList(topicPartition));

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
            final TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, cluster.nodeById(0), cluster.nodes(), Collections.<Node>emptyList());
            adminClient.addTopic(false, TOPIC, Collections.singletonList(topicPartitionInfo), null);
            streamsResetter.doDelete(Collections.singletonList(TOPIC), adminClient);
            assertEquals(Collections.emptySet(), adminClient.listTopics().names().get());
        }
    }

    @Test
    public void shouldDetermineInternalTopicBasedOnTopicName1() {
        assertTrue(streamsResetter.matchesInternalTopicFormat("appId-named-subscription-response-topic"));
        assertTrue(streamsResetter.matchesInternalTopicFormat("appId-named-subscription-registration-topic"));
        assertTrue(streamsResetter.matchesInternalTopicFormat("appId-KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-12323232-topic"));
        assertTrue(streamsResetter.matchesInternalTopicFormat("appId-KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-12323232-topic"));
    }

    private Cluster createCluster(final int numNodes) {
        final HashMap<Integer, Node> nodes = new HashMap<>();
        for (int i = 0; i < numNodes; ++i) {
            nodes.put(i, new Node(i, "localhost", 8121 + i));
        }
        return new Cluster("mockClusterId", nodes.values(),
            Collections.<PartitionInfo>emptySet(), Collections.<String>emptySet(),
            Collections.<String>emptySet(), nodes.get(0));
    }

}