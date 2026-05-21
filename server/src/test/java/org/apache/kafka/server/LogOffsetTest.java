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
package org.apache.kafka.server;

import kafka.server.KafkaBroker;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.FileRecords;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.storage.internals.log.LogStartOffsetIncrementReason;
import org.apache.kafka.storage.internals.log.OffsetResultHolder;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.test.TestUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static org.apache.kafka.server.TestUtils.awaitLeaderChange;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    serverProperties = {
        @ClusterConfigProperty(key = "log.flush.interval.messages", value = "1"),
        @ClusterConfigProperty(key = "num.partitions", value = "20"),
        @ClusterConfigProperty(key = "log.retention.hours", value = "10"),
        @ClusterConfigProperty(key = "log.retention.check.interval.ms", value = "300000")
    }
)
public class LogOffsetTest {

    private final ClusterInstance clusterInstance;

    LogOffsetTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @ClusterTest
    public void testGetOffsetsForUnknownTopic() throws IOException {
        TopicPartition topicPartition = new TopicPartition("foo", 0);
        ListOffsetsRequest request = ListOffsetsRequest.Builder
            .forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
            .setTargetTimes(buildTargetTimes(topicPartition, ListOffsetsRequest.LATEST_TIMESTAMP))
            .build((short) 1);
        ListOffsetsResponse response = sendListOffsetsRequest(request);
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), findPartition(response.topics(), topicPartition).errorCode());
    }

    @ClusterTest
    public void testGetOffsetsAfterDeleteRecords() throws Exception {
        String topic = "kafka-";
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        UnifiedLog log = createTopicAndGetLog(topic, topicPartition);

        for (int i = 0; i < 20; i++) {
            log.appendAsLeader(singletonRecords("42".getBytes()), 0);
        }
        log.flush(false);

        log.updateHighWatermark(log.logEndOffset());
        log.maybeIncrementLogStartOffset(3L, LogStartOffsetIncrementReason.ClientRecordDeletion);
        log.deleteOldSegments();

        Optional<Long> offset = log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Optional.empty())
            .timestampAndOffsetOpt().map(t -> t.offset);
        assertEquals(Optional.of(20L), offset);

        awaitLeaderChange(clusterInstance, topicPartition, Optional.of(broker().config().brokerId()));
        ListOffsetsRequest request = ListOffsetsRequest.Builder
            .forReplica((short) 1, 1)
            .setTargetTimes(buildTargetTimes(topicPartition, ListOffsetsRequest.LATEST_TIMESTAMP))
            .build();
        long consumerOffset = findPartition(sendListOffsetsRequest(request).topics(), topicPartition).offset();
        assertEquals(20L, consumerOffset);
    }

    @ClusterTest
    public void testFetchOffsetByTimestampForMaxTimestampAfterTruncate() throws Exception {
        String topic = "kafka-";
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        UnifiedLog log = createTopicAndGetLog(topic, topicPartition);

        for (int timestamp = 0; timestamp < 20; timestamp++) {
            log.appendAsLeader(singletonRecords("42".getBytes(), (long) timestamp), 0);
        }
        log.flush(false);

        log.updateHighWatermark(log.logEndOffset());

        Optional<FileRecords.TimestampAndOffset> firstOffset = log
            .fetchOffsetByTimestamp(ListOffsetsRequest.MAX_TIMESTAMP, Optional.empty())
            .timestampAndOffsetOpt();
        assertEquals(19L, firstOffset.get().offset);
        assertEquals(19L, firstOffset.get().timestamp);

        log.truncateTo(0);

        assertEquals(Optional.empty(),
            log.fetchOffsetByTimestamp(ListOffsetsRequest.MAX_TIMESTAMP, Optional.empty()).timestampAndOffsetOpt());
    }

    @ClusterTest
    public void testFetchOffsetByTimestampForMaxTimestampWithUnorderedTimestamps() throws Exception {
        String topic = "kafka-";
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        UnifiedLog log = createTopicAndGetLog(topic, topicPartition);

        for (long timestamp : List.of(0L, 1L, 2L, 3L, 4L, 6L, 5L)) {
            log.appendAsLeader(singletonRecords("42".getBytes(), timestamp), 0);
        }
        log.flush(false);

        log.updateHighWatermark(log.logEndOffset());

        Optional<FileRecords.TimestampAndOffset> maxTimestampOffset = log
            .fetchOffsetByTimestamp(ListOffsetsRequest.MAX_TIMESTAMP, Optional.empty())
            .timestampAndOffsetOpt();
        assertEquals(7L, log.logEndOffset());
        assertEquals(5L, maxTimestampOffset.get().offset);
        assertEquals(6L, maxTimestampOffset.get().timestamp);
    }

    @ClusterTest
    public void testGetOffsetsBeforeLatestTime() throws Exception {
        String topic = "kafka-";
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        UnifiedLog log = createTopicAndGetLog(topic, topicPartition);

        Uuid topicId = getTopicId(topic);
        Map<Uuid, String> topicNames = Map.of(topicId, topic);

        for (int i = 0; i < 20; i++) {
            log.appendAsLeader(singletonRecords("42".getBytes()), 0);
        }
        log.flush(false);

        Optional<Long> offset = log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Optional.empty())
            .timestampAndOffsetOpt().map(t -> t.offset);
        assertEquals(Optional.of(20L), offset);

        awaitLeaderChange(clusterInstance, topicPartition, Optional.of(broker().config().brokerId()));
        ListOffsetsRequest request = ListOffsetsRequest.Builder
            .forReplica((short) 1, 1)
            .setTargetTimes(buildTargetTimes(topicPartition, ListOffsetsRequest.LATEST_TIMESTAMP))
            .build();
        long consumerOffset = findPartition(sendListOffsetsRequest(request).topics(), topicPartition).offset();
        assertEquals(20L, consumerOffset);

        // try to fetch using latest offset
        FetchRequest fetchRequest = FetchRequest.Builder.forConsumer(
            ApiKeys.FETCH.latestVersion(), 0, 1,
            Map.of(topicPartition, new FetchRequest.PartitionData(topicId, consumerOffset,
                FetchRequest.INVALID_LOG_START_OFFSET, 300 * 1024, Optional.empty()))
        ).build();
        FetchResponse fetchResponse = sendFetchRequest(fetchRequest);
        assertFalse(FetchResponse.recordsOrFail(
            fetchResponse.responseData(topicNames, ApiKeys.FETCH.latestVersion()).get(topicPartition))
                .batches().iterator().hasNext());
    }

    @ClusterTest
    public void testEmptyLogsGetOffsets() throws Exception {
        String topic = "kafka-";
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        clusterInstance.createTopic(topic, 1, (short) 1);

        ListOffsetsRequest request = ListOffsetsRequest.Builder
            .forReplica((short) 1, 1)
            .setTargetTimes(buildTargetTimes(topicPartition, ListOffsetsRequest.EARLIEST_TIMESTAMP))
            .build();
        long consumerOffset = findPartition(sendListOffsetsRequest(request).topics(), topicPartition).offset();
        assertEquals(0L, consumerOffset);
    }

    @ClusterTest
    public void testFetchOffsetByTimestampForMaxTimestampWithEmptyLog() throws Exception {
        String topic = "kafka-";
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        UnifiedLog log = createTopicAndGetLog(topic, topicPartition);

        log.updateHighWatermark(log.logEndOffset());

        assertEquals(0L, log.logEndOffset());
        assertEquals(new OffsetResultHolder(), log.fetchOffsetByTimestamp(ListOffsetsRequest.MAX_TIMESTAMP, Optional.empty()));
    }

    @ClusterTest
    public void testGetOffsetsBeforeEarliestTime() throws Exception {
        Random random = new Random();
        String topic = "kafka-";
        TopicPartition topicPartition = new TopicPartition(topic, random.nextInt(3));

        clusterInstance.createTopic(topic, 3, (short) 1);

        UnifiedLog log = broker().logManager().getOrCreateLog(topicPartition, Optional.empty());
        for (int i = 0; i < 20; i++) {
            log.appendAsLeader(singletonRecords("42".getBytes()), 0);
        }
        log.flush(false);

        Optional<Long> offset = log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP, Optional.empty())
            .timestampAndOffsetOpt().map(t -> t.offset);
        assertEquals(Optional.of(0L), offset);

        awaitLeaderChange(clusterInstance, topicPartition, Optional.of(broker().config().brokerId()));
        ListOffsetsRequest request = ListOffsetsRequest.Builder
            .forReplica((short) 1, 1)
            .setTargetTimes(buildTargetTimes(topicPartition, ListOffsetsRequest.EARLIEST_TIMESTAMP))
            .build();
        long offsetFromResponse = findPartition(sendListOffsetsRequest(request).topics(), topicPartition).offset();
        assertEquals(0L, offsetFromResponse);
    }

    private KafkaBroker broker() {
        return clusterInstance.aliveBrokers().values().iterator().next();
    }

    private ListOffsetsResponse sendListOffsetsRequest(ListOffsetsRequest request) throws IOException {
        return IntegrationTestUtils.connectAndReceive(request, clusterInstance.brokerBoundPorts().get(0));
    }

    private FetchResponse sendFetchRequest(FetchRequest request) throws IOException {
        return IntegrationTestUtils.connectAndReceive(request, clusterInstance.brokerBoundPorts().get(0));
    }

    private List<ListOffsetsTopic> buildTargetTimes(TopicPartition tp, long timestamp) {
        return List.of(new ListOffsetsTopic()
            .setName(tp.topic())
            .setPartitions(List.of(new ListOffsetsPartition()
                .setPartitionIndex(tp.partition())
                .setTimestamp(timestamp))));
    }

    private ListOffsetsPartitionResponse findPartition(List<ListOffsetsTopicResponse> topics, TopicPartition tp) {
        return topics.stream()
            .filter(t -> t.name().equals(tp.topic())).findFirst().get()
            .partitions().stream()
            .filter(p -> p.partitionIndex() == tp.partition()).findFirst().get();
    }

    private UnifiedLog createTopicAndGetLog(String topic, TopicPartition topicPartition) throws Exception {
        clusterInstance.createTopic(topic, 1, (short) 1);

        TestUtils.waitForCondition(() -> broker().logManager().getLog(topicPartition).isPresent(),
            "Log for partition [topic,0] should be created");
        return broker().logManager().getLog(topicPartition).get();
    }

    private Uuid getTopicId(String topic) throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            return admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic).topicId();
        }
    }

    private static MemoryRecords singletonRecords(byte[] value) {
        return MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(value));
    }

    private static MemoryRecords singletonRecords(byte[] value, long timestamp) {
        return MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(timestamp, null, value));
    }
}
