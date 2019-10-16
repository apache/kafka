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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class StreamsResetterTest {

    private static final String TOPIC = "topic1";
    private final StreamsResetter streamsResetter = new StreamsResetter();
    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final TopicPartition topicPartition = new TopicPartition(TOPIC, 0);
    private final Set<TopicPartition> inputTopicPartitions = new HashSet<>(Collections.singletonList(topicPartition));

    @Before
    public void setUp() {
        consumer.assign(Collections.singletonList(topicPartition));
    }

    @Test
    public void testResetToSpecificOffsetWhenBetweenBeginningAndEndOffset() {
        IntStream.range(0, 5).forEach(i -> consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), i, new byte[0], new byte[0])));

        streamsResetter.resetOffsetsTo(consumer, inputTopicPartitions, 2L);

        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        assertEquals(3, records.count());
    }

    @Test
    public void testResetToSpecificOffsetWhenBeforeBeginningOffset() {
        IntStream.range(3, 6).forEach(i -> consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), i, new byte[0], new byte[0])));

        streamsResetter.resetOffsetsTo(consumer, inputTopicPartitions, 2L);

        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        assertEquals(3, records.count());
    }

    @Test
    public void testResetToSpecificOffsetWhenAfterEndOffset() {
        IntStream.range(0, 3).forEach(i -> consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), i, new byte[0], new byte[0])));

        streamsResetter.resetOffsetsTo(consumer, inputTopicPartitions, 4L);

        //Emulate new message received after reset
        consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), 3, new byte[0], new byte[0]));
        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        assertEquals(1, records.count());
        assertEquals(3, records.records(topicPartition).get(0).offset());
    }

    @Test
    public void testShiftOffsetByWhenBetweenBeginningAndEndOffset() {
        IntStream.range(0, 5).forEach(i -> consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), i, new byte[0], new byte[0])));

        streamsResetter.shiftOffsetsBy(consumer, inputTopicPartitions, 3L);

        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        assertEquals(2, records.count());
    }

    @Test
    public void testShiftOffsetByWhenBeforeBeginningOffset() {
        IntStream.range(0, 5).forEach(i -> consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), i, new byte[0], new byte[0])));

        streamsResetter.shiftOffsetsBy(consumer, inputTopicPartitions, -3L);

        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        assertEquals(5, records.count());
    }

    @Test
    public void testShiftOffsetByWhenAfterEndOffset() {
        IntStream.range(0, 3).forEach(i -> consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), i, new byte[0], new byte[0])));

        streamsResetter.shiftOffsetsBy(consumer, inputTopicPartitions, 5L);

        //Emulate new message received after reset
        consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), 3, new byte[0], new byte[0]));
        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        assertEquals(1, records.count());
        assertEquals(3, records.records(topicPartition).get(0).offset());
    }

    @Test
    public void testResetUsingPlanWhenBetweenBeginningAndEndOffset() {
        IntStream.range(0, 5).forEach(i -> consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), i, new byte[0], new byte[0])));

        final Map<TopicPartition, Long> topicPartitionsAndOffset = new HashMap<>();
        topicPartitionsAndOffset.put(topicPartition, 3L);
        streamsResetter.resetOffsetsFromResetPlan(consumer, inputTopicPartitions, topicPartitionsAndOffset);

        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        assertEquals(2, records.count());
    }

    @Test
    public void testResetUsingPlanWhenBeforeBeginningOffset() {
        IntStream.range(3, 6).forEach(i -> consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), i, new byte[0], new byte[0])));

        final Map<TopicPartition, Long> topicPartitionsAndOffset = new HashMap<>();
        topicPartitionsAndOffset.put(topicPartition, 1L);
        streamsResetter.resetOffsetsFromResetPlan(consumer, inputTopicPartitions, topicPartitionsAndOffset);

        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        assertEquals(3, records.count());
    }

    @Test
    public void testResetUsingPlanWhenAfterEndOffset() {
        IntStream.range(0, 3).forEach(i -> consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), i, new byte[0], new byte[0])));

        final Map<TopicPartition, Long> topicPartitionsAndOffset = new HashMap<>();
        topicPartitionsAndOffset.put(topicPartition, 5L);
        streamsResetter.resetOffsetsFromResetPlan(consumer, inputTopicPartitions, topicPartitionsAndOffset);

        //Emulate new message received after reset
        consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), 3, new byte[0], new byte[0]));
        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        assertEquals(1, records.count());
        assertEquals(3, records.records(topicPartition).get(0).offset());
    }

    @Test
    public void shouldSeekToEndOffset() {
        IntStream.range(0, 3).forEach(i -> consumer.addRecord(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), i, new byte[0], new byte[0])));

        final Set<TopicPartition> intermediateTopicPartitions = new HashSet<>();
        intermediateTopicPartitions.add(topicPartition);
        streamsResetter.maybeSeekToEnd("g1", consumer, intermediateTopicPartitions);

        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        assertEquals(0, records.count());
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

    private Cluster createCluster(final int numNodes) {
        final HashMap<Integer, Node> nodes = new HashMap<>();
        for (int i = 0; i < numNodes; ++i) {
            nodes.put(i, new Node(i, "localhost", 8121 + i));
        }
        return new Cluster("mockClusterId", nodes.values(),
            Collections.<PartitionInfo>emptySet(), Collections.<String>emptySet(),
            Collections.<String>emptySet(), nodes.get(0));
    }

    @Test
    public void shouldAcceptValidDateFormats() throws ParseException {
        //check valid formats
        invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"));
        invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
        invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX"));
        invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXX"));
        invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"));
    }

    @Test
    public void shouldThrowOnInvalidDateFormat() throws ParseException {
        //check some invalid formats
        try {
            invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"));
            fail("Call to getDateTime should fail");
        } catch (final Exception e) {
            e.printStackTrace();
        }

        try {
            invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.X"));
            fail("Call to getDateTime should fail");
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    private void invokeGetDateTimeMethod(final SimpleDateFormat format) throws ParseException {
        final Date checkpoint = new Date();
        final StreamsResetter streamsResetter = new StreamsResetter();
        final String formattedCheckpoint = format.format(checkpoint);
        streamsResetter.getDateTime(formattedCheckpoint);
    }
}