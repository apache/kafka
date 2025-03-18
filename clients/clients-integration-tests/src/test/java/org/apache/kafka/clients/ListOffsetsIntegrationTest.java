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
package org.apache.kafka.clients;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import scala.jdk.javaapi.CollectionConverters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 3,
    serverProperties = {
        @ClusterConfigProperty(key = "log.retention.ms", value = "-1"),
    }
)
public class ListOffsetsIntegrationTest {
    private static final String TOPIC = "topic";
    private static final String CUSTOM_CONFIG_TOPIC = "custom_topic";
    private static final short REPLICAS = 1;
    private static final int PARTITION = 1;
    private Admin adminClient;
    // FIXME: new infra does not support mocktime, this change is need?
    private Time mockTime = new MockTime(1);
    private ClusterInstance clusterInstance;

    ListOffsetsIntegrationTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @BeforeEach
    public void setup() throws InterruptedException {
        clusterInstance.waitForReadyBrokers();
        clusterInstance.createTopic(TOPIC, PARTITION, REPLICAS);
        adminClient = clusterInstance.admin();
    }

    @AfterEach
    public void teardown() {
        Utils.closeQuietly(adminClient, "ListOffsetsAdminClient");
    }

    @ClusterTest
    public void testListMaxTimestampWithEmptyLog() throws InterruptedException, ExecutionException {
        ListOffsetsResultInfo maxTimestampOffset = runFetchOffsets(adminClient, OffsetSpec.maxTimestamp(), TOPIC);
        assertEquals(ListOffsetsResponse.UNKNOWN_OFFSET, maxTimestampOffset.offset());
        assertEquals(ListOffsetsResponse.UNKNOWN_TIMESTAMP, maxTimestampOffset.timestamp());
    }

    @ClusterTest
    public void testThreeCompressedRecordsInOneBatch() throws InterruptedException, ExecutionException {
        produceMessagesInOneBatch("gzip", TOPIC);
        verifyListOffsets(TOPIC, 1);

        // test LogAppendTime case
        setUpForLogAppendTimeCase();
        produceMessagesInOneBatch("gzip", CUSTOM_CONFIG_TOPIC);
        // In LogAppendTime's case, the maxTimestampOffset should be the first message of the batch.
        // So in this one batch test, it'll be the first offset 0
        verifyListOffsets(CUSTOM_CONFIG_TOPIC, 0);
    }

    @ClusterTest
    public void testThreeNonCompressedRecordsInOneBatch() throws ExecutionException, InterruptedException {
        produceMessagesInOneBatch("none", TOPIC);
        verifyListOffsets(TOPIC, 1);

        // test LogAppendTime case
        setUpForLogAppendTimeCase();
        produceMessagesInOneBatch("none", CUSTOM_CONFIG_TOPIC);
        // In LogAppendTime's case, if the timestamps are the same, we choose the offset of the first record
        // thus, the maxTimestampOffset should be the first record of the batch.
        // So in this one batch test, it'll be the first offset which is 0
        verifyListOffsets(CUSTOM_CONFIG_TOPIC, 0);
    }

    @ClusterTest
    public void testThreeNonCompressedRecordsInSeparateBatch() throws ExecutionException, InterruptedException {
        produceMessagesInOneBatch("none", TOPIC);
        verifyListOffsets(TOPIC, 1);

        // test LogAppendTime case
        setUpForLogAppendTimeCase();
        produceMessagesInSeparateBatch("none", CUSTOM_CONFIG_TOPIC);
        // In LogAppendTime's case, if the timestamp is different, it should be the last one
        verifyListOffsets(CUSTOM_CONFIG_TOPIC, 2);
    }

    @ClusterTest
    public void testThreeRecordsInOneBatchHavingDifferentCompressionTypeWithServer() throws InterruptedException, ExecutionException {
        createTopicWithConfig(CUSTOM_CONFIG_TOPIC, Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"));
        produceMessagesInOneBatch("none", CUSTOM_CONFIG_TOPIC);
        verifyListOffsets(CUSTOM_CONFIG_TOPIC, 1);
    }

    @ClusterTest
    public void testThreeRecordsInSeparateBatchHavingDifferentCompressionTypeWithServer() throws InterruptedException, ExecutionException {
        createTopicWithConfig(CUSTOM_CONFIG_TOPIC, Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"));
        produceMessagesInOneBatch("none", CUSTOM_CONFIG_TOPIC);
        verifyListOffsets(CUSTOM_CONFIG_TOPIC, 1);
    }

    @ClusterTest
    public void testThreeCompressedRecordsInSeparateBatch() throws InterruptedException, ExecutionException {
        produceMessagesInSeparateBatch("none", TOPIC);
        verifyListOffsets(TOPIC, 1);

        // test LogAppendTime case
        setUpForLogAppendTimeCase();
        produceMessagesInSeparateBatch("gzip", CUSTOM_CONFIG_TOPIC);
        // In LogAppendTime's case, the maxTimestampOffset is the message in the last batch since we advance the time
        // for each batch, So it'll be the last offset 2
        verifyListOffsets(CUSTOM_CONFIG_TOPIC, 2);
    }

    private void produceMessagesInOneBatch(String compressionType, String topic) {
        List<ProducerRecord<byte[], byte[]>> records = new LinkedList<>();
        records.add(new ProducerRecord<>(topic, 0, 100L, null, new byte[10]));
        records.add(new ProducerRecord<>(topic, 0, 999L, null, new byte[10]));
        records.add(new ProducerRecord<>(topic, 0, 200L, null, new byte[10]));

        // create a producer with large linger.ms and enough batch.size (default is enough for three 10 bytes records),
        // so that we can confirm all records will be accumulated in producer until we flush them into one batch.
        try (Producer<byte[], byte[]> producer = clusterInstance.producer(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers(),
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE,
                ProducerConfig.LINGER_MS_CONFIG, Integer.MAX_VALUE,
                ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType))) {

            List<Future<RecordMetadata>> futures = records.stream().map(record -> producer.send(record)).toList();
            producer.flush();

            for (Future<RecordMetadata> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    fail();
                }
            }
        }
    }

    private void produceMessagesInSeparateBatch(String compressionType, String topic) throws ExecutionException, InterruptedException {
        ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(topic, 0, 100L, null, new byte[10]);
        ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(topic, 0, 999L, null, new byte[10]);
        ProducerRecord<byte[], byte[]> record3 = new ProducerRecord<>(topic, 0, 200L, null, new byte[10]);

        // create a producer with large linger.ms and enough batch.size (default is enough for three 10 bytes records),
        // so that we can confirm all records will be accumulated in producer until we flush them into one batch.
        try (Producer<byte[], byte[]> producer = clusterInstance.producer(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers(),
                ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType))) {

            Future<RecordMetadata> future1 = producer.send(record1);
            future1.get();
            // advance the server time after each record sent to make sure the time changed when appendTime is used
            mockTime.sleep(100);

            Future<RecordMetadata> future2 = producer.send(record2);
            future2.get();
            mockTime.sleep(100);

            Future<RecordMetadata> future3 = producer.send(record3);
            future3.get();
        }
    }

    private void verifyListOffsets(String topic, int expectedMaxTimestampOffset) throws ExecutionException, InterruptedException {

        // case 0: test the offsets from leader's append path
        checkListOffsets(adminClient, topic, expectedMaxTimestampOffset);

        // case 1: test the offsets from follower's append path.
        // we make a follower be the new leader to handle the ListOffsetRequest
        int previousLeader = clusterInstance.getLeaderBrokerId(new TopicPartition(topic, 0));
        int newLeader = clusterInstance.brokerIds().stream().filter(id -> id != previousLeader).findAny().get();

        // change the leader to new one
        adminClient.alterPartitionReassignments(
            Map.of(new TopicPartition(topic, 0), Optional.of(new NewPartitionReassignment(List.of(newLeader))))
        ).all().get();
        // wait for all reassignments get completed
        TestUtils.waitForCondition(() -> {
            try {
                return adminClient.listPartitionReassignments().reassignments().get().isEmpty();
            } catch (InterruptedException | ExecutionException e) {
                return false;
            }
        }, "There still are ongoing reassignments");
        // make sure we are able to see the new leader
        TestUtils.waitForCondition(() -> {
            try {
                return clusterInstance.getLeaderBrokerId(new TopicPartition(topic, 0)) == newLeader;
            } catch (InterruptedException | ExecutionException e) {
                return false;
            }
        }, String.format("expected leader: %d but actual: %d", newLeader, clusterInstance.getLeaderBrokerId(new TopicPartition(topic, 0))));
        checkListOffsets(adminClient, topic, expectedMaxTimestampOffset);

        // case 2: test the offsets from recovery path.
        // server will rebuild offset index according to log files if the index files are nonexistent
        Set<String> indexFiles = clusterInstance.brokers().values().stream().flatMap(broker ->
                CollectionConverters.asJava(broker.config().logDirs()).stream()
        ).collect(Collectors.toUnmodifiableSet());
        clusterInstance.brokers().values().stream().forEach(b -> b.shutdown());
        indexFiles.forEach(root -> {
            File[] files = new File(String.format("%s/$s-0", root, topic)).listFiles();
            if (files != null)
                Arrays.stream(files).forEach(f -> {
                    if (f.getName().endsWith(".index"))
                        f.delete();
                });
        });

        clusterInstance.brokers().values().stream().forEach(b -> {
            if (b.isShutdown())
                b.startup();
        });

        Utils.closeQuietly(adminClient, "ListOffsetsAdminClient");
        adminClient = clusterInstance.admin(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()));
        checkListOffsets(adminClient, topic, expectedMaxTimestampOffset);
    }

    private static void checkListOffsets(Admin admin, String topic, int expectedMaxTimestampOffset) throws ExecutionException, InterruptedException {
        ListOffsetsResultInfo earliestOffset = runFetchOffsets(admin, OffsetSpec.earliest(), topic);
        assertEquals(0, earliestOffset.offset());

        ListOffsetsResultInfo latestOffset = runFetchOffsets(admin, OffsetSpec.latest(), topic);
        assertEquals(3, latestOffset.offset());

        ListOffsetsResultInfo maxTimestampOffset = runFetchOffsets(admin, OffsetSpec.maxTimestamp(), topic);
        assertEquals(expectedMaxTimestampOffset, maxTimestampOffset.offset());

        // the epoch is related to the returned offset.
        // Hence, it should be zero (the earliest leader epoch), regardless of new leader election
        assertEquals(Optional.of(0), maxTimestampOffset.leaderEpoch());
    }

    private static ListOffsetsResultInfo runFetchOffsets(Admin adminClient,
                                                OffsetSpec offsetSpec,
                                                String topic) throws InterruptedException, ExecutionException {
        TopicPartition tp = new TopicPartition(topic, 0);
        return adminClient.listOffsets(Map.of(tp, offsetSpec), new ListOffsetsOptions()).all().get().get(tp);
    }

    private void setUpForLogAppendTimeCase() throws InterruptedException {
        createTopicWithConfig(CUSTOM_CONFIG_TOPIC, Map.of(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "LogAppendTime"));
    }

    private void createTopicWithConfig(String topic, Map<String, String> props) throws InterruptedException {
        clusterInstance.createTopic(topic, PARTITION, REPLICAS, props);
    }
}
